"""
verifier_core.py — agroverify 핵심 검수 로직
==============================================

설계 (사용자 요청 v0.1):
  - 비교 단위: (시장, 일자)   ← 법인 차원 제거
  - Source of truth: agromarket.kr/domeinfo/marketTrade.do
  - 비교 대상: C:\\agro_data_v2\\daily\\<year>\\agro_<yyyymmdd>.db (read-only)
  - 영업일만 (월~금)
  - innong 본체 의존 X (agro_db import 불필요)

API:
  fetch_market_day(yyyymmdd) -> {market_name: {amount, qty}}
  fetch_local_day(yyyymmdd)  -> {market_name: {amount, qty}}
  compare_day(yyyymmdd)      -> list[dict]
  run_verify(start, end, threshold, log_cb, progress_cb, stop_cb) -> dict
"""
from __future__ import annotations
import os
import re
import glob
import sqlite3
import requests
import urllib3
from datetime import date, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL = 'https://at.agromarket.kr/domeinfo/marketTrade.do'
DEFAULT_AGRO_ROOT = r'C:\agro_data_v2'

# tr 패턴: 시장 / 법인 / qty / amount(원)
_ROW_RE = re.compile(
    r'<tr>\s*'
    r'<td>([^<]+)</td>\s*'           # 시장명
    r'<td>([^<]+)</td>\s*'           # 법인명 (합산용으로만 파싱)
    r'<td[^>]*>([\d,]+)</td>\s*'     # qty
    r'<td[^>]*>.*?<span>([\d,]+)</span>',   # amount (원)
    re.DOTALL
)


def _ymd_to_dash(yyyymmdd: str) -> str:
    return f'{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}'


def fetch_market_day(yyyymmdd: str, timeout: int = 30) -> dict:
    """agromarket.kr 의 특정 일자 시장별 합계 (법인은 합산).

    Returns: {market_name: {'amount': int, 'qty': int}}
    """
    d = _ymd_to_dash(yyyymmdd)
    params = {
        'startDateBefore': d, 'endDateBefore': d,
        'startDate': d, 'endDate': d,
        'largeCdBefore': '', 'midCdBefore': '',
        'largeCd': '', 'midCd': '',
    }
    try:
        r = requests.get(URL, params=params, timeout=timeout, verify=False,
                         headers={'User-Agent': 'Mozilla/5.0 (agroverify)'})
        if r.status_code != 200:
            return {}
        html = r.text
    except Exception:
        return {}

    out = {}
    for m in _ROW_RE.finditer(html):
        market = (m.group(1) or '').strip()
        try:
            qty = int(m.group(3).replace(',', ''))
            amount = int(m.group(4).replace(',', ''))
        except Exception:
            continue
        if market not in out:
            out[market] = {'amount': 0, 'qty': 0}
        out[market]['amount'] += amount
        out[market]['qty']    += qty
    return out


def fetch_local_day(yyyymmdd: str, agro_root: str = DEFAULT_AGRO_ROOT) -> dict:
    """우리 daily DB 의 특정 일자 시장별 합계."""
    yr = yyyymmdd[:4]
    p = os.path.join(agro_root, 'daily', yr, f'agro_{yyyymmdd}.db')
    if not os.path.isfile(p):
        return {}
    out = {}
    try:
        dc = sqlite3.connect(f'file:{p}?mode=ro', uri=True)
        for r in dc.execute("""SELECT market_name,
                                      IFNULL(SUM(amount), 0),
                                      IFNULL(SUM(qty),    0)
                               FROM agro_trades
                               GROUP BY market_name"""):
            mk = r[0] or ''
            out[mk] = {'amount': float(r[1] or 0), 'qty': float(r[2] or 0)}
        dc.close()
    except Exception:
        pass
    return out


def compare_day(yyyymmdd: str, agro_root: str = DEFAULT_AGRO_ROOT,
                threshold_pct: float = 5.0) -> list:
    """특정 일자 시장 단위 비교.

    Returns: [{
        'ymd', 'market',
        'remote_amt', 'local_amt', 'remote_qty', 'local_qty',
        'amt_ratio_pct', 'qty_ratio_pct',
        'amt_diff', 'qty_diff',
        'flag': 'ok' | 'short' | 'over' | 'missing' | 'orphan',
    }, ...]   (amt_diff 절대값 내림차순)
    """
    remote = fetch_market_day(yyyymmdd)
    local  = fetch_local_day(yyyymmdd, agro_root=agro_root)
    diff = []
    keys = set(remote.keys()) | set(local.keys())
    for k in keys:
        r = remote.get(k, {'amount': 0, 'qty': 0})
        l = local.get(k,  {'amount': 0, 'qty': 0})
        r_amt, l_amt = r['amount'], l['amount']
        r_qty, l_qty = r['qty'],    l['qty']
        amt_ratio = (l_amt / r_amt * 100) if r_amt > 0 else 0.0
        qty_ratio = (l_qty / r_qty * 100) if r_qty > 0 else 0.0
        if r_amt == 0 and l_amt > 0:
            flag = 'orphan'   # 우리만 있음 (의심)
        elif r_amt > 0 and l_amt == 0:
            flag = 'missing'  # agromarket 만 있음 (우리 누락)
        elif abs(amt_ratio - 100) < threshold_pct:
            flag = 'ok'
        elif amt_ratio < (100 - threshold_pct):
            flag = 'short'    # 우리 부족
        else:
            flag = 'over'     # 우리 부풀림
        diff.append({
            'ymd':           yyyymmdd,
            'market':        k,
            'remote_amt':    r_amt, 'local_amt': l_amt,
            'remote_qty':    r_qty, 'local_qty': l_qty,
            'amt_ratio_pct': amt_ratio,
            'qty_ratio_pct': qty_ratio,
            'amt_diff':      l_amt - r_amt,
            'qty_diff':      l_qty - r_qty,
            'flag':          flag,
        })
    diff.sort(key=lambda x: -abs(x['amt_diff']))
    return diff


def _iter_business_days(start_yyyymmdd: str, end_yyyymmdd: str):
    sy, sm, sd = int(start_yyyymmdd[:4]), int(start_yyyymmdd[4:6]), int(start_yyyymmdd[6:8])
    ey, em, ed = int(end_yyyymmdd[:4]),   int(end_yyyymmdd[4:6]),   int(end_yyyymmdd[6:8])
    d = date(sy, sm, sd)
    end = date(ey, em, ed)
    while d <= end:
        if d.weekday() < 5:   # 월(0) ~ 금(4)
            yield d.strftime('%Y%m%d')
        d += timedelta(days=1)


def run_verify(start_yyyymmdd: str, end_yyyymmdd: str,
               threshold_pct: float = 5.0,
               agro_root: str = DEFAULT_AGRO_ROOT,
               log_cb=None,
               progress_cb=None,
               stop_cb=None) -> dict:
    """전체 기간 일자별 검수.

    Args:
      start/end:      'YYYYMMDD'
      threshold_pct:  이 이상 차이나면 problem 카운트 (default 5%)
      agro_root:      C:\\agro_data_v2 경로
      log_cb(str):           로그 콜백 (GUI 텍스트 패널)
      progress_cb(done, total, msg): 진행률 콜백
      stop_cb() -> bool:     True 반환 시 중단

    Returns: {
      'days_checked', 'days_with_problem',
      'problem_rows',   # [{ymd, market, remote_amt, local_amt, ...}, ...]
      'top10',          # [...]
      'started_at', 'finished_at', 'stopped': bool,
    }
    """
    log = log_cb or (lambda msg: None)
    from datetime import datetime

    days = list(_iter_business_days(start_yyyymmdd, end_yyyymmdd))
    total = len(days)
    started = datetime.now()
    log(f'🔍 검수 시작 — 기간 {start_yyyymmdd}~{end_yyyymmdd}, 영업일 {total}일, 임계 {threshold_pct}%')
    log(f'    데이터 루트: {agro_root}')

    problem_rows = []
    days_with_problem = 0
    days_done = 0
    days_no_local = 0    # 우리 DB 없는 영업일
    stopped = False

    for i, ymd in enumerate(days):
        if stop_cb and stop_cb():
            log('⏹ 사용자 중단 요청')
            stopped = True
            break

        # 우리 DB 파일 존재 확인
        local_db = os.path.join(agro_root, 'daily', ymd[:4], f'agro_{ymd}.db')
        if not os.path.isfile(local_db):
            days_no_local += 1
            if progress_cb:
                try: progress_cb(i+1, total, f'{ymd} (DB 없음, skip)')
                except: pass
            continue

        try:
            day_diff = compare_day(ymd, agro_root=agro_root, threshold_pct=threshold_pct)
        except Exception as ex:
            log(f'  ⚠ {ymd} 비교 실패: {ex}')
            if progress_cb:
                try: progress_cb(i+1, total, f'{ymd} 실패')
                except: pass
            continue

        # 차이 row 만 누적
        day_problems = [d for d in day_diff
                        if d['flag'] in ('short', 'over', 'missing', 'orphan')]
        if day_problems:
            days_with_problem += 1
            problem_rows.extend(day_problems)

        days_done += 1
        # 매 영업일마다 1줄 로그 (사용자가 진행 보이게)
        n_ok = sum(1 for d in day_diff if d['flag'] == 'ok')
        log(f'  [{i+1:>4}/{total}] {ymd} — 시장 {len(day_diff)} (ok {n_ok} / 차이 {len(day_problems)})')

        if progress_cb:
            try: progress_cb(i+1, total, f'{ymd}')
            except: pass

    finished = datetime.now()
    elapsed = (finished - started).total_seconds()

    # TOP 10 (절대 금액 차이 큰 순)
    top10 = sorted(problem_rows, key=lambda x: -abs(x['amt_diff']))[:10]

    log('')
    log('=' * 70)
    log(f'🎯 검수 완료 — 소요 {elapsed:.0f}초')
    log(f'  영업일 검사: {days_done} / 전체 {total} (DB 없음 skip {days_no_local})')
    log(f'  문제 발견 영업일: {days_with_problem}')
    log(f'  문제 row(시장×일자) 누적: {len(problem_rows)}')
    if top10:
        log(f'  ── TOP 10 (금액 차이 큰 순) ──')
        for d in top10:
            ic = {'short':'⬇', 'over':'⬆', 'missing':'❌', 'orphan':'🟠'}.get(d['flag'], '?')
            log(f'    {ic} {d["ymd"]} {d["market"][:12]:<12} '
                f'agro {d["remote_amt"]/1e8:>6.2f}억 / 우리 {d["local_amt"]/1e8:>6.2f}억 '
                f'({d["amt_ratio_pct"]:.0f}%)')

    return {
        'days_checked':      days_done,
        'days_total':        total,
        'days_no_local':     days_no_local,
        'days_with_problem': days_with_problem,
        'problem_rows':      problem_rows,
        'top10':             top10,
        'started_at':        started.isoformat(),
        'finished_at':       finished.isoformat(),
        'elapsed_sec':       elapsed,
        'stopped':           stopped,
        'threshold_pct':     threshold_pct,
        'period':            f'{start_yyyymmdd}~{end_yyyymmdd}',
    }


# ── CLI ──
if __name__ == '__main__':
    import sys, argparse, json
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description='agroverify CLI (no GUI)')
    ap.add_argument('start', help='YYYYMMDD')
    ap.add_argument('end',   help='YYYYMMDD')
    ap.add_argument('--threshold', type=float, default=5.0)
    ap.add_argument('--agro-root', default=DEFAULT_AGRO_ROOT)
    ap.add_argument('--output', help='결과 JSON 저장 경로')
    args = ap.parse_args()

    res = run_verify(args.start, args.end,
                     threshold_pct=args.threshold,
                     agro_root=args.agro_root,
                     log_cb=print)
    if args.output:
        out_p = os.path.abspath(args.output)
        os.makedirs(os.path.dirname(out_p), exist_ok=True)
        with open(out_p, 'w', encoding='utf-8') as f:
            json.dump(res, f, ensure_ascii=False, indent=2, default=str)
        print(f'\n💾 결과 저장: {out_p}')
