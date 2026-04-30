"""
run.py — agroverify GUI
=========================

사용자 요구사항:
  - innong 본체와 분리, 자체 Tkinter GUI
  - 실시간 진행 로그 패널 (매 영업일 1줄)
  - 진행률 바 + 시작/중지 버튼
  - 결과 TOP 10 테이블
  - 결과 JSON 저장 (data/results/)
  - 안전: thread-safe 로깅 (root.after), 중지 깔끔히
"""
from __future__ import annotations
import os
import sys
import json
import threading
import queue
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# 같은 폴더의 verifier_core import
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
import verifier_core as vc   # noqa: E402

APP_NAME = 'agroverify'
RESULT_DIR = os.path.join(HERE, 'data', 'results')
os.makedirs(RESULT_DIR, exist_ok=True)

_BG  = '#1e1e1e'
_BG2 = '#2a2a2a'
_FG  = '#e0e0e0'
_FG2 = '#9e9e9e'
_AC  = '#42a5f5'


def _read_version():
    p = os.path.join(HERE, 'VERSION')
    try:
        with open(p, encoding='utf-8') as f:
            return f.read().strip()
    except Exception:
        return '?'


def _default_dates():
    """오늘 기준 디폴트 — 이번 달 1일 ~ 어제."""
    from datetime import date, timedelta
    today = date.today()
    yesterday = today - timedelta(days=1)
    first_of_month = today.replace(day=1)
    return first_of_month.strftime('%Y%m%d'), yesterday.strftime('%Y%m%d')


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(f'{APP_NAME} v{_read_version()} — agromarket.kr ↔ agro_data_v2 검수')
        self.root.geometry('1100x720')
        self.root.configure(bg=_BG)

        self._stop = threading.Event()
        self._thread = None
        self._log_queue = queue.Queue()
        self._last_result = None

        self._build_ui()
        self._poll_log_queue()

    # ── UI ──
    def _build_ui(self):
        # 상단 컨트롤 바
        top = tk.Frame(self.root, bg=_BG2, pady=8, padx=12)
        top.pack(fill='x')

        tk.Label(top, text=f'🔍 {APP_NAME}', bg=_BG2, fg=_AC,
                 font=(None, 14, 'bold')).pack(side='left', padx=(0, 16))

        tk.Label(top, text='시작:', bg=_BG2, fg=_FG).pack(side='left')
        s, e = _default_dates()
        self.ent_s = tk.Entry(top, width=11, bg=_BG, fg=_FG, insertbackground=_FG)
        self.ent_s.insert(0, s)
        self.ent_s.pack(side='left', padx=(4, 12))

        tk.Label(top, text='끝:', bg=_BG2, fg=_FG).pack(side='left')
        self.ent_e = tk.Entry(top, width=11, bg=_BG, fg=_FG, insertbackground=_FG)
        self.ent_e.insert(0, e)
        self.ent_e.pack(side='left', padx=(4, 12))

        tk.Label(top, text='임계(%):', bg=_BG2, fg=_FG).pack(side='left')
        self.ent_t = tk.Entry(top, width=6, bg=_BG, fg=_FG, insertbackground=_FG)
        self.ent_t.insert(0, '5.0')
        self.ent_t.pack(side='left', padx=(4, 12))

        tk.Label(top, text='데이터 루트:', bg=_BG2, fg=_FG).pack(side='left')
        self.ent_root = tk.Entry(top, width=22, bg=_BG, fg=_FG, insertbackground=_FG)
        self.ent_root.insert(0, vc.DEFAULT_AGRO_ROOT)
        self.ent_root.pack(side='left', padx=(4, 12))

        self.btn_start = tk.Button(top, text='▶ 시작', bg='#1976d2', fg='white',
                                    font=(None, 10, 'bold'), padx=10,
                                    command=self._on_start)
        self.btn_start.pack(side='left', padx=4)
        self.btn_stop = tk.Button(top, text='⏹ 중지', bg='#c62828', fg='white',
                                   font=(None, 10, 'bold'), padx=10,
                                   command=self._on_stop, state='disabled')
        self.btn_stop.pack(side='left', padx=4)

        self.btn_save = tk.Button(top, text='💾 결과 저장', bg=_BG, fg=_FG,
                                   command=self._on_save_result, state='disabled')
        self.btn_save.pack(side='right', padx=4)
        self.btn_open_dir = tk.Button(top, text='📁 결과 폴더', bg=_BG, fg=_FG,
                                       command=self._on_open_dir)
        self.btn_open_dir.pack(side='right', padx=4)

        # 진행 바
        prog_frame = tk.Frame(self.root, bg=_BG, pady=4, padx=12)
        prog_frame.pack(fill='x')
        self.lbl_progress = tk.Label(prog_frame, text='대기 중', bg=_BG, fg=_FG2,
                                      anchor='w')
        self.lbl_progress.pack(fill='x')
        self.pb = ttk.Progressbar(prog_frame, mode='determinate', maximum=100)
        self.pb.pack(fill='x', pady=(2, 4))

        # 본문: 좌(로그) / 우(TOP10) — PanedWindow
        body = tk.PanedWindow(self.root, orient='horizontal',
                               bg=_BG, sashwidth=4)
        body.pack(fill='both', expand=True, padx=12, pady=(0, 8))

        # 로그 패널
        log_frame = tk.LabelFrame(body, text='실시간 로그', bg=_BG2, fg=_FG,
                                   padx=4, pady=4)
        body.add(log_frame, minsize=400)
        log_inner = tk.Frame(log_frame, bg=_BG2)
        log_inner.pack(fill='both', expand=True)
        self.log_txt = tk.Text(log_inner, bg=_BG, fg=_FG, insertbackground=_FG,
                                wrap='none', font=('Consolas', 9))
        ysb = tk.Scrollbar(log_inner, orient='vertical', command=self.log_txt.yview)
        xsb = tk.Scrollbar(log_inner, orient='horizontal', command=self.log_txt.xview)
        self.log_txt.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
        self.log_txt.grid(row=0, column=0, sticky='nsew')
        ysb.grid(row=0, column=1, sticky='ns')
        xsb.grid(row=1, column=0, sticky='ew')
        log_inner.grid_rowconfigure(0, weight=1)
        log_inner.grid_columnconfigure(0, weight=1)

        # 결과 TOP 패널
        right = tk.LabelFrame(body, text='TOP 10 (금액 차이 큰 순)', bg=_BG2, fg=_FG,
                               padx=4, pady=4)
        body.add(right, minsize=380)
        cols = ('flag', 'ymd', 'market', 'remote', 'local', 'ratio')
        self.tv = ttk.Treeview(right, columns=cols, show='headings', height=12)
        widths = {'flag': 50, 'ymd': 80, 'market': 110, 'remote': 90,
                  'local': 90, 'ratio': 60}
        labels = {'flag': '플래그', 'ymd': '일자', 'market': '시장',
                  'remote': 'agro(억)', 'local': '우리(억)', 'ratio': '비율%'}
        for c in cols:
            self.tv.heading(c, text=labels[c])
            self.tv.column(c, width=widths[c],
                            anchor='e' if c in ('remote', 'local', 'ratio') else 'w')
        self.tv.pack(fill='both', expand=True)

        # 하단 요약
        self.lbl_summary = tk.Label(self.root, text='', bg=_BG, fg=_FG2,
                                     anchor='w', padx=12, pady=4)
        self.lbl_summary.pack(fill='x')

        # 초기 안내
        self._log_text(
            f'━━ {APP_NAME} v{_read_version()} ━━\n'
            f' • 비교: agromarket.kr (marketTrade.do) ↔ {vc.DEFAULT_AGRO_ROOT}\n'
            f' • 단위: 시장 × 일자 (영업일만, 법인 차원 X)\n'
            f' • 우리 DB 파일이 없는 영업일은 skip (DB 없음으로 카운트)\n'
            f' • 매 영업일마다 1줄 로그가 찍힘 → 실시간 확인 가능\n'
            f' • [중지] 누르면 다음 영업일 진입 전에 깔끔히 종료\n\n'
        )

    # ── 로그/진행 ──
    def _log_text(self, msg: str):
        """Tk 메인 스레드에서만 호출."""
        self.log_txt.insert('end', msg if msg.endswith('\n') else (msg + '\n'))
        self.log_txt.see('end')

    def _log_safe(self, msg: str):
        """워커 스레드에서 호출 — 큐에 넣고 메인이 폴링."""
        self._log_queue.put(('log', msg))

    def _progress_safe(self, done: int, total: int, msg: str):
        self._log_queue.put(('progress', (done, total, msg)))

    def _poll_log_queue(self):
        try:
            while True:
                kind, payload = self._log_queue.get_nowait()
                if kind == 'log':
                    self._log_text(payload)
                elif kind == 'progress':
                    done, total, msg = payload
                    pct = (done / total * 100) if total > 0 else 0
                    self.pb['value'] = pct
                    self.lbl_progress.configure(
                        text=f'진행 {done}/{total} ({pct:.1f}%) — {msg}')
                elif kind == 'done':
                    self._on_finished(payload)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_log_queue)

    # ── 시작/중지 ──
    def _on_start(self):
        if self._thread and self._thread.is_alive():
            messagebox.showwarning('실행 중', '이미 검수가 진행 중입니다. 중지 후 다시 시작하세요.')
            return
        s = self.ent_s.get().strip()
        e = self.ent_e.get().strip()
        try:
            thr = float(self.ent_t.get().strip() or '5.0')
        except ValueError:
            messagebox.showerror('입력 오류', '임계 % 는 숫자만 입력하세요.')
            return
        agro_root = self.ent_root.get().strip() or vc.DEFAULT_AGRO_ROOT

        if len(s) != 8 or len(e) != 8 or not (s.isdigit() and e.isdigit()):
            messagebox.showerror('입력 오류', '날짜는 YYYYMMDD 8자리 숫자.')
            return
        if s > e:
            messagebox.showerror('입력 오류', '시작이 끝보다 큽니다.')
            return
        if not os.path.isdir(agro_root):
            if not messagebox.askyesno('경로 확인',
                f'데이터 루트가 존재하지 않습니다:\n{agro_root}\n\n그래도 진행하시겠습니까?\n(모든 영업일이 "DB 없음"으로 skip 됩니다)'):
                return

        # UI 상태
        self.btn_start.configure(state='disabled')
        self.btn_stop.configure(state='normal')
        self.btn_save.configure(state='disabled')
        self._stop.clear()
        self.pb['value'] = 0
        # log 패널 헤더만 추가 (이전 내역 보존)
        self._log_text('\n' + '═' * 70)
        self._log_text(f'▶ START [{datetime.now():%Y-%m-%d %H:%M:%S}] '
                       f'{s}~{e} threshold={thr}%')
        self._log_text('═' * 70)
        # tv 비우기
        for r in self.tv.get_children():
            self.tv.delete(r)

        # 워커 스레드
        self._thread = threading.Thread(
            target=self._worker,
            args=(s, e, thr, agro_root),
            daemon=True
        )
        self._thread.start()

    def _on_stop(self):
        self._stop.set()
        self.btn_stop.configure(state='disabled')
        self._log_safe('⏹ 중지 요청 — 현재 영업일 끝나면 중단됩니다...')

    def _worker(self, s, e, thr, agro_root):
        try:
            res = vc.run_verify(
                start_yyyymmdd=s, end_yyyymmdd=e,
                threshold_pct=thr,
                agro_root=agro_root,
                log_cb=self._log_safe,
                progress_cb=self._progress_safe,
                stop_cb=lambda: self._stop.is_set(),
            )
            self._log_queue.put(('done', res))
        except Exception as ex:
            import traceback
            self._log_safe(f'❌ 워커 예외: {ex}')
            self._log_safe(traceback.format_exc())
            self._log_queue.put(('done', None))

    def _on_finished(self, res: dict | None):
        self.btn_start.configure(state='normal')
        self.btn_stop.configure(state='disabled')

        if not res:
            self.lbl_progress.configure(text='실패 또는 예외 발생')
            return

        self._last_result = res
        self.btn_save.configure(state='normal')

        # 자동 저장
        try:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            auto_path = os.path.join(
                RESULT_DIR,
                f'verify_{res["period"].replace("~","-")}_{ts}.json')
            with open(auto_path, 'w', encoding='utf-8') as f:
                json.dump(res, f, ensure_ascii=False, indent=2, default=str)
            self._log_text(f'💾 자동 저장: {auto_path}')
        except Exception as ex:
            self._log_text(f'⚠ 자동 저장 실패: {ex}')

        # TOP10 테이블
        for r in self.tv.get_children():
            self.tv.delete(r)
        icon = {'short': '⬇', 'over': '⬆', 'missing': '❌', 'orphan': '🟠'}
        for d in res.get('top10', []):
            self.tv.insert('', 'end', values=(
                icon.get(d['flag'], '?'),
                d['ymd'],
                d['market'][:18],
                f'{d["remote_amt"]/1e8:.2f}',
                f'{d["local_amt"]/1e8:.2f}',
                f'{d["amt_ratio_pct"]:.0f}',
            ))

        # 요약
        self.lbl_summary.configure(text=(
            f'  검사 {res["days_checked"]}/{res["days_total"]} 영업일 '
            f'(DB 없음 skip {res["days_no_local"]}) · '
            f'문제 발견 영업일 {res["days_with_problem"]} · '
            f'문제 row {len(res["problem_rows"])} · '
            f'소요 {res["elapsed_sec"]:.0f}초'
            f'{" · ⏹ 중단됨" if res.get("stopped") else ""}'
        ))
        self.lbl_progress.configure(text='완료')
        self.pb['value'] = 100

    def _on_save_result(self):
        if not self._last_result:
            messagebox.showinfo('알림', '저장할 결과가 없습니다.')
            return
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        default = f'verify_{self._last_result["period"].replace("~","-")}_{ts}.json'
        p = filedialog.asksaveasfilename(
            initialdir=RESULT_DIR,
            initialfile=default,
            defaultextension='.json',
            filetypes=[('JSON', '*.json'), ('All', '*.*')],
        )
        if not p: return
        try:
            with open(p, 'w', encoding='utf-8') as f:
                json.dump(self._last_result, f, ensure_ascii=False,
                           indent=2, default=str)
            messagebox.showinfo('저장 완료', p)
        except Exception as ex:
            messagebox.showerror('저장 실패', str(ex))

    def _on_open_dir(self):
        try:
            os.startfile(RESULT_DIR)   # type: ignore
        except Exception as ex:
            messagebox.showerror('열기 실패', str(ex))


def main():
    root = tk.Tk()
    # ttk 스타일 (Treeview 다크)
    style = ttk.Style()
    try: style.theme_use('clam')
    except Exception: pass
    style.configure('Treeview', background=_BG, foreground=_FG,
                    fieldbackground=_BG, rowheight=22)
    style.configure('Treeview.Heading', background=_BG2, foreground=_FG)
    style.map('Treeview', background=[('selected', '#1565c0')])

    App(root)
    root.mainloop()


if __name__ == '__main__':
    main()
