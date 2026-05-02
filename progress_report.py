"""
progress_report.py — agroverify full run 진행 + 일치율 보고
=========================================================

매시간 schtasks 로 호출되어:
  1. 최신 auto_fix_full_*.log 파싱
  2. PID alive 확인
  3. 일치율 / 진행률 추출
  4. progress.md 작성 (G드라이브 + 로컬)
  5. shared_log 한 줄 push

Usage:
  python progress_report.py [--final]
"""
from __future__ import annotations
import os, re, sys, json, glob, subprocess
from datetime import datetime
from pathlib import Path

WORKDIR    = Path(r'C:\LimTools-apps\agroverify')
RESULTS    = WORKDIR / 'data' / 'results'
PID_FILE   = RESULTS / 'auto_fix_full.pid'
DRIVE_DST  = Path(r'G:\내 드라이브\agroverify_handoff\results')
SHARED_LOG_HELPER = Path(r'C:\Users\인농\siri_engine\scripts\claude_log.py')

PAT_MONTH       = re.compile(r'━━ \[(\d+)/(\d+)\] (\d{4}-\d{2}) ━━')
PAT_TARGETS     = re.compile(r'📋 처리 대상 (\d+) 시장')
PAT_PASS        = re.compile(r'✅ 검증 통과 .*?시장 (\d+).*?합계 비율 ([\d.]+)%')
PAT_REVERIFY    = re.compile(r'↻ 재검증: 비율 ([\d.]+)% \(이전 ([\d.]+)%\)')
PAT_DOWN        = re.compile(r'🔽 \[(\d+)\] (\S+) \((.+?)\) 재다운')
PAT_DOWN_FAIL   = re.compile(r'❌ 다운 실패')
PAT_RELOAD      = re.compile(r'📂 받은 XLS (\d+) 개')
PAT_UNVERIF     = re.compile(r'검증 불가 월 → (\d+) 시장 다운')
PAT_DAILY_OK    = re.compile(r'✅ 저장: (\d{4}-\d{2}-\d{2})')
PAT_FINAL       = re.compile(r'🎯 auto_fix 완료')

def find_latest_log() -> Path | None:
    files = sorted(RESULTS.glob('auto_fix_full_*.log'))
    return files[-1] if files else None

def is_pid_alive(pid: int) -> bool:
    try:
        out = subprocess.run(['tasklist', '/FI', f'PID eq {pid}', '/FO', 'CSV', '/NH'],
                             capture_output=True, text=True, timeout=10)
        return f'"{pid}"' in out.stdout
    except Exception:
        return False

def parse_log(log_path: Path) -> dict:
    txt = log_path.read_text(encoding='utf-8', errors='replace')
    months_seen     = []   # [(idx, total, label)]
    pass_months     = []   # [(label, n_market, ratio)]
    reverify_months = []   # [(label, before, after)]
    targets_per_mo  = {}   # label -> count (last seen)
    download_count  = 0
    download_fails  = 0
    reload_files    = 0
    daily_saves     = 0
    unverif_fired   = 0
    finished        = bool(PAT_FINAL.search(txt))

    cur_label = None
    for line in txt.splitlines():
        m = PAT_MONTH.search(line)
        if m:
            cur_label = m.group(3)
            months_seen.append((int(m.group(1)), int(m.group(2)), cur_label))
            continue
        m = PAT_TARGETS.search(line)
        if m and cur_label:
            targets_per_mo[cur_label] = int(m.group(1))
            continue
        m = PAT_PASS.search(line)
        if m and cur_label:
            pass_months.append((cur_label, int(m.group(1)), float(m.group(2))))
            continue
        m = PAT_REVERIFY.search(line)
        if m and cur_label:
            after = float(m.group(1)); before = float(m.group(2))
            reverify_months.append((cur_label, before, after))
            continue
        if PAT_DOWN.search(line):     download_count  += 1
        if PAT_DOWN_FAIL.search(line):download_fails  += 1
        m = PAT_RELOAD.search(line)
        if m: reload_files += int(m.group(1))
        if PAT_DAILY_OK.search(line): daily_saves += 1
        m = PAT_UNVERIF.search(line)
        if m: unverif_fired += 1

    cur_idx, cur_total = (months_seen[-1][0], months_seen[-1][1]) if months_seen else (0, 0)
    last_label = months_seen[-1][2] if months_seen else None

    return {
        'log_path': str(log_path),
        'log_size': log_path.stat().st_size,
        'mtime': datetime.fromtimestamp(log_path.stat().st_mtime).isoformat(timespec='seconds'),
        'finished': finished,
        'cur_idx': cur_idx, 'cur_total': cur_total, 'cur_label': last_label,
        'months_seen_n': len(months_seen),
        'pass_months': pass_months,           # already-OK 월 (검증 통과)
        'reverify_months': reverify_months,   # 다운+적재 후 재검증 월
        'targets_per_mo': targets_per_mo,
        'download_count': download_count,
        'download_fails': download_fails,
        'reload_files': reload_files,
        'daily_saves': daily_saves,
        'unverif_fired': unverif_fired,
    }

def get_pid() -> int | None:
    if not PID_FILE.exists():
        return None
    try:
        return int(PID_FILE.read_text().strip())
    except Exception:
        return None

def fmt_md(parsed: dict, pid: int | None, alive: bool) -> str:
    now = datetime.now().isoformat(timespec='seconds')
    elapsed_pct = (parsed['cur_idx'] / parsed['cur_total'] * 100) if parsed['cur_total'] else 0

    # 평균 일치율 (재검증 + 통과 모두)
    ratios_after = [a for (_, _, a) in parsed['reverify_months']] + \
                   [r for (_, _, r) in parsed['pass_months']]
    avg_after = sum(ratios_after) / len(ratios_after) if ratios_after else 0

    avg_before = (sum(b for (_, b, _) in parsed['reverify_months'])
                  / len(parsed['reverify_months'])) if parsed['reverify_months'] else 0

    md = []
    md.append(f'# agroverify full run 진행 보고')
    md.append('')
    md.append(f'**갱신**: {now}')
    md.append(f'**PID**: {pid}  **상태**: {"🟢 alive" if alive else "🔴 DEAD/완료"}')
    md.append(f'**완료 마커**: {"✅ 있음" if parsed["finished"] else "⏳ 아직"}')
    md.append('')
    md.append('## 진행률')
    md.append('')
    md.append(f'- 처리 월: **[{parsed["cur_idx"]}/{parsed["cur_total"]}]** ({elapsed_pct:.1f}%)')
    md.append(f'- 마지막 처리: **{parsed["cur_label"]}**')
    md.append(f'- 검증 불가 월 다운 발동: {parsed["unverif_fired"]} 건')
    md.append('')
    md.append('## 일치율 (핵심)')
    md.append('')
    md.append(f'- 검증 통과 월: **{len(parsed["pass_months"])}** 건')
    md.append(f'- 재검증 (다운+적재 후) 월: **{len(parsed["reverify_months"])}** 건')
    md.append(f'- **평균 사후 일치율**: **{avg_after:.1f}%**')
    if parsed['reverify_months']:
        md.append(f'- 재검증 평균: 이전 {avg_before:.1f}% → 이후 {avg_after:.1f}% '
                  f'(개선 {avg_after-avg_before:+.1f}%p)')
    md.append('')
    md.append('## 다운로드 / 적재')
    md.append('')
    md.append(f'- 시장 단위 재다운 시도: {parsed["download_count"]} 건 (실패 {parsed["download_fails"]})')
    md.append(f'- 일별 다운 성공: {parsed["daily_saves"]} 일자')
    md.append(f'- 적재 파일 누적: {parsed["reload_files"]} XLS')
    md.append('')

    # 재검증 월 표 (최근 30개)
    if parsed['reverify_months']:
        md.append('## 재검증 일치율 (최근 30개월)')
        md.append('')
        md.append('| 월 | 이전 | 이후 | 변화 |')
        md.append('|---|---:|---:|---:|')
        for (lab, b, a) in parsed['reverify_months'][-30:]:
            md.append(f'| {lab} | {b:.1f}% | {a:.1f}% | {a-b:+.1f}%p |')
        md.append('')

    # 통과 월 표 (최근 20)
    if parsed['pass_months']:
        md.append('## 검증 통과 월 (재다운 불필요, 최근 20)')
        md.append('')
        md.append('| 월 | 시장 수 | 일치율 |')
        md.append('|---|---:|---:|')
        for (lab, n, r) in parsed['pass_months'][-20:]:
            md.append(f'| {lab} | {n} | {r:.1f}% |')
        md.append('')

    md.append('## 메타')
    md.append('')
    md.append(f'- log 파일: `{parsed["log_path"]}`')
    md.append(f'- log 크기: {parsed["log_size"]:,} bytes')
    md.append(f'- log 마지막 쓰기: {parsed["mtime"]}')
    md.append('')
    return '\n'.join(md)

def push_shared_log(line: str):
    if not SHARED_LOG_HELPER.exists(): return
    try:
        subprocess.run(['python', str(SHARED_LOG_HELPER), 'add', line,
                        '--tag', 'office-server / Claude / agroverify auto_fix 진행보고'],
                       capture_output=True, timeout=30)
    except Exception:
        pass

def main():
    is_final = '--final' in sys.argv

    log = find_latest_log()
    if not log:
        print('NO LOG FOUND'); return 1

    pid = get_pid()
    alive = is_pid_alive(pid) if pid else False

    parsed = parse_log(log)
    md = fmt_md(parsed, pid, alive)

    # 저장
    DRIVE_DST.mkdir(parents=True, exist_ok=True)
    out_drive = DRIVE_DST / 'progress.md'
    out_local = RESULTS / 'progress.md'
    out_drive.write_text(md, encoding='utf-8')
    out_local.write_text(md, encoding='utf-8')

    # 결과 / 로그 미러
    for src in [log, RESULTS / log.with_suffix('.json').name]:
        if src.exists():
            dst = DRIVE_DST / src.name
            try:
                dst.write_bytes(src.read_bytes())
            except Exception as ex:
                print(f'mirror fail {src.name}: {ex}')

    # shared_log 한 줄
    avg_after = sum(a for *_, a in parsed['reverify_months']) / len(parsed['reverify_months']) if parsed['reverify_months'] else 0
    line = (f"- agroverify auto_fix [{parsed['cur_idx']}/{parsed['cur_total']}] "
            f"{parsed['cur_label']}, "
            f"통과 {len(parsed['pass_months'])} / 재검증 {len(parsed['reverify_months'])} (사후 평균 {avg_after:.1f}%), "
            f"다운 {parsed['download_count']}건 (실패 {parsed['download_fails']}) / "
            f"적재 {parsed['reload_files']} XLS / 일자 {parsed['daily_saves']}, "
            f"PID {pid} {'alive' if alive else 'DEAD'} "
            f"{'(✅ 완료)' if parsed['finished'] else ''}")
    push_shared_log(line)

    print(line)
    print(f'\nWrote: {out_drive}')
    print(f'Wrote: {out_local}')

    # 완료/사망 감지 시 추가 동작
    if (parsed['finished'] or not alive) and is_final:
        print('\n🎯 FINAL: full run completed or died')

    return 0

if __name__ == '__main__':
    sys.exit(main())
