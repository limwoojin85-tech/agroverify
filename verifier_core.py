"""
verifier_core.py — agroverify 핵심 검수 로직
==============================================

설계 v0.3 (사용자 명시):
  agromarket "도매시장별 통계정보" URL = 시장+법인 row 의 표
  vs
  우리 DB (= agro_loader 적재 = C:\\agro_data_v2\\daily) 의
  같은 기간 GROUP BY market_name, corp_name

  → 비교 단위: **(시장, 법인)**
  → 영업일만 (월~금)
  → 표기 일치 확인 결과 정규화 불필요 (㈜, (공) 까지 그대로 일치)

API:
  fetch_remote_period_corp(start, end) -> {(market, corp): {amount, qty}}
  fetch_local_period_corp (start, end) -> {(market, corp): {amount, qty}}
  compare_period(start_yyyymmdd, end_yyyymmdd) -> dict (전체 합계 + 시장×법인 list)
  run_verify(start, end, ...) -> dict (월별 분해 옵션)
"""
from __future__ import annotations
import os
import re
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
    r'<td>([^<]+)</td>\s*'
    r'<td>([^<]+)</td>\s*'
    r'<td[^>]*>([\d,]+)</td>\s*'
    r'<td[^>]*>.*?<span>([\d,]+)</span>',
    re.DOTALL
)


def _ymd_to_dash(yyyymmdd: str) -> str:
    return f'{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}'


def fetch_remote_period_corp(start_yyyymmdd: str, end_yyyymmdd: str,
                              timeout: int = 60) -> dict:
    """agromarket URL 한 번 호출 → (시장, 법인) 단위 합계.
    Returns: {(market, corp): {'amount': int, 'qty': int}}, plus '_meta' key.
    """
    s = _ymd_to_dash(start_yyyymmdd)
    e = _ymd_to_dash(end_yyyymmdd)
    params = {
        'startDateBefore': s, 'endDateBefore': e,
        'startDate': s, 'endDate': e,
        'largeCdBefore': '', 'midCdBefore': '',
        'largeCd': '', 'midCd': '',
    }
    out = {}
    meta = {'http_status': None, 'n_rows': 0, 'url': '', 'error': None}
    try:
        r = requests.get(URL, params=params, timeout=timeout, verify=False,
                         headers={'User-Agent': 'Mozilla/5.0 (agroverify)'})
        meta['http_status'] = r.status_code
        meta['url'] = r.url
        if r.status_code != 200:
            meta['error'] = f'HTTP {r.status_code}'
            return {'_meta': meta}
        html = r.text
    except Exception as ex:
        meta['error'] = str(ex)
        return {'_meta': meta}

    for m in _ROW_RE.finditer(html):
        market = (m.group(1) or '').strip()
        corp   = (m.group(2) or '').strip()
        try:
            qty = int(m.group(3).replace(',', ''))
            amount = int(m.group(4).replace(',', ''))
        except Exception:
            continue
        key = (market, corp)
        if key not in out:
            out[key] = {'amount': 0, 'qty': 0}
        out[key]['amount'] += amount
        out[key]['qty']    += qty
        meta['n_rows'] += 1
    out['_meta'] = meta
    return out


def _iter_business_days(start_yyyymmdd: str, end_yyyymmdd: str):
    sy, sm, sd = int(start_yyyymmdd[:4]), int(start_yyyymmdd[4:6]), int(start_yyyymmdd[6:8])
    ey, em, ed = int(end_yyyymmdd[:4]),   int(end_yyyymmdd[4:6]),   int(end_yyyymmdd[6:8])
    d = date(sy, sm, sd)
    end = date(ey, em, ed)
    while d <= end:
        if d.weekday() < 5:
            yield d.strftime('%Y%m%d')
        d += timedelta(days=1)


def fetch_local_period_corp(start_yyyymmdd: str, end_yyyymmdd: str,
                             agro_root: str = DEFAULT_AGRO_ROOT) -> dict:
    """우리 daily DB 같은 기간 GROUP BY market_name, corp_name SUM.
    Returns: {(market, corp): {'amount', 'qty'}}, plus '_meta'.
    """
    out = {}
    meta = {'n_files_found': 0, 'n_files_missing': 0}
    for ymd in _iter_business_days(start_yyyymmdd, end_yyyymmdd):
        p = os.path.join(agro_root, 'daily', ymd[:4], f'agro_{ymd}.db')
        if not os.path.isfile(p):
            meta['n_files_missing'] += 1
            continue
        try:
            dc = sqlite3.connect(f'file:{p}?mode=ro', uri=True)
            for r in dc.execute("""SELECT market_name, corp_name,
                                          IFNULL(SUM(amount),0),
                                          IFNULL(SUM(qty),0)
                                   FROM agro_trades
                                   GROUP BY market_name, corp_name"""):
                key = ((r[0] or '').strip(), (r[1] or '').strip())
                if key not in out:
                    out[key] = {'amount': 0.0, 'qty': 0.0}
                out[key]['amount'] += float(r[2] or 0)
                out[key]['qty']    += float(r[3] or 0)
            dc.close()
            meta['n_files_found'] += 1
        except Exception:
            meta['n_files_missing'] += 1
    out['_meta'] = meta
    return out


def compare_period(start_yyyymmdd: str, end_yyyymmdd: str,
                   agro_root: str = DEFAULT_AGRO_ROOT,
                   threshold_pct: float = 5.0) -> dict:
    """기간 (시장, 법인) 단위 비교.

    Returns: {
      'period', 'totals': {remote_amt, local_amt, remote_qty, local_qty,
                           amt_ratio_pct, qty_ratio_pct, amt_diff, qty_diff},
      'rows': [{market, corp, remote_amt, local_amt, ..., flag}, ...],
              # 차이 절대값 큰 순
      'remote_meta', 'local_meta',
    }
    flag: 'ok' | 'short' | 'over' | 'missing' | 'orphan'
      missing : agromarket 에만 있음 (우리 누락) ★
      orphan  : 우리 DB 에만 있음 (의심)
      short   : 우리가 임계보다 부족
      over    : 우리가 임계보다 과다
      ok      : 임계 이내
    """
    remote = fetch_remote_period_corp(start_yyyymmdd, end_yyyymmdd)
    local  = fetch_local_period_corp(start_yyyymmdd, end_yyyymmdd, agro_root=agro_root)
    rmeta  = remote.pop('_meta', {})
    lmeta  = local.pop('_meta', {})

    keys = set(remote.keys()) | set(local.keys())
    rows = []
    tot_r_amt = tot_l_amt = 0.0
    tot_r_qty = tot_l_qty = 0.0
    for k in keys:
        r = remote.get(k, {'amount': 0, 'qty': 0})
        l = local.get(k,  {'amount': 0, 'qty': 0})
        r_amt = r['amount']; l_amt = l['amount']
        r_qty = r['qty'];    l_qty = l['qty']
        tot_r_amt += r_amt; tot_l_amt += l_amt
        tot_r_qty += r_qty; tot_l_qty += l_qty
        amt_ratio = (l_amt / r_amt * 100) if r_amt > 0 else 0.0
        qty_ratio = (l_qty / r_qty * 100) if r_qty > 0 else 0.0
        if r_amt == 0 and l_amt > 0:
            flag = 'orphan'
        elif r_amt > 0 and l_amt == 0:
            flag = 'missing'
        elif abs(amt_ratio - 100) < threshold_pct:
            flag = 'ok'
        elif amt_ratio < (100 - threshold_pct):
            flag = 'short'
        else:
            flag = 'over'
        rows.append({
            'market':        k[0],
            'corp':          k[1],
            'remote_amt':    r_amt, 'local_amt': l_amt,
            'remote_qty':    r_qty, 'local_qty': l_qty,
            'amt_ratio_pct': amt_ratio,
            'qty_ratio_pct': qty_ratio,
            'amt_diff':      l_amt - r_amt,
            'qty_diff':      l_qty - r_qty,
            'flag':          flag,
        })
    rows.sort(key=lambda x: -abs(x['amt_diff']))

    totals = {
        'remote_amt': tot_r_amt, 'local_amt': tot_l_amt,
        'remote_qty': tot_r_qty, 'local_qty': tot_l_qty,
        'amt_diff':   tot_l_amt - tot_r_amt,
        'qty_diff':   tot_l_qty - tot_r_qty,
        'amt_ratio_pct': (tot_l_amt / tot_r_amt * 100) if tot_r_amt > 0 else 0,
        'qty_ratio_pct': (tot_l_qty / tot_r_qty * 100) if tot_r_qty > 0 else 0,
    }
    summary_flags = {
        'ok':      sum(1 for r in rows if r['flag'] == 'ok'),
        'short':   sum(1 for r in rows if r['flag'] == 'short'),
        'over':    sum(1 for r in rows if r['flag'] == 'over'),
        'missing': sum(1 for r in rows if r['flag'] == 'missing'),
        'orphan':  sum(1 for r in rows if r['flag'] == 'orphan'),
    }
    return {
        'period':       f'{start_yyyymmdd}~{end_yyyymmdd}',
        'totals':       totals,
        'rows':         rows,
        'flag_counts':  summary_flags,
        'remote_meta':  rmeta,
        'local_meta':   lmeta,
    }


def run_verify(start_yyyymmdd: str, end_yyyymmdd: str,
               agro_root: str = DEFAULT_AGRO_ROOT,
               with_monthly_breakdown: bool = True,
               log_cb=None,
               progress_cb=None,
               stop_cb=None,
               threshold_pct: float = 5.0,
               top_n_rows: int = 30) -> dict:
    """기간 (시장, 법인) 단위 비교 + 옵션 월별 분해."""
    log = log_cb or (lambda msg: None)
    from datetime import datetime
    started = datetime.now()

    log(f'🔍 검수 시작 — {start_yyyymmdd} ~ {end_yyyymmdd}')
    log(f'    데이터 루트: {agro_root}')
    log(f'    비교 단위 : (시장, 법인)')
    log(f'    월별 분해 : {"ON" if with_monthly_breakdown else "OFF"}')
    log('')

    # 1) 기간 전체
    log(f'  [전체 기간] agromarket 호출 중...')
    if progress_cb:
        try: progress_cb(0, 1, '전체 기간 호출')
        except: pass
    period_total = compare_period(start_yyyymmdd, end_yyyymmdd, agro_root=agro_root,
                                   threshold_pct=threshold_pct)
    t = period_total['totals']
    fc = period_total['flag_counts']
    log(f'  agromarket: {len(period_total["rows"])} (시장,법인) 키, '
        f'{t["remote_amt"]/1e8:>10,.2f} 억원, {t["remote_qty"]/1000:>10,.1f} 톤')
    log(f'  우리 DB   : {fc["ok"]+fc["short"]+fc["over"]+fc["orphan"]} 키, '
        f'{t["local_amt"]/1e8:>10,.2f} 억원, {t["local_qty"]/1000:>10,.1f} 톤')
    log(f'  비율(금액): {t["amt_ratio_pct"]:.2f}%, 차이 {t["amt_diff"]/1e8:+,.2f} 억')
    log(f'  비율(물량): {t["qty_ratio_pct"]:.2f}%, 차이 {t["qty_diff"]/1000:+,.1f} 톤')
    log(f'  플래그    : ok={fc["ok"]}  short={fc["short"]}  over={fc["over"]}  '
        f'❌missing={fc["missing"]}  🟠orphan={fc["orphan"]}')
    if period_total['remote_meta'].get('error'):
        log(f'  ⚠ remote 에러: {period_total["remote_meta"]["error"]}')
    log('')

    # 2) (시장, 법인) TOP N
    log(f'─ (시장, 법인) TOP {top_n_rows} (금액 차이 절대값 큰 순) ─')
    log(f'{"flg":>3} {"시장":<10} {"법인":<18} {"agro(억)":>10} {"우리(억)":>10} {"비율%":>7} {"차이(억)":>10}')
    log('─' * 78)
    icon = {'ok':'  ', 'short':'⬇ ', 'over':'⬆ ', 'missing':'❌', 'orphan':'🟠'}
    for d in period_total['rows'][:top_n_rows]:
        log(f'{icon.get(d["flag"],"?"):>3} {d["market"][:10]:<10} {d["corp"][:18]:<18} '
            f'{d["remote_amt"]/1e8:>10,.2f} {d["local_amt"]/1e8:>10,.2f} '
            f'{d["amt_ratio_pct"]:>7.1f} {d["amt_diff"]/1e8:>+10,.2f}')
    log('')

    # 3) 옵션: 월별 분해 (전체 totals 만, 시장×법인 detail 은 큼)
    monthly = []
    if with_monthly_breakdown:
        sy, sm = int(start_yyyymmdd[:4]), int(start_yyyymmdd[4:6])
        ey, em = int(end_yyyymmdd[:4]),   int(end_yyyymmdd[4:6])
        from calendar import monthrange
        months = []
        cy, cm = sy, sm
        while (cy, cm) <= (ey, em):
            mlast = monthrange(cy, cm)[1]
            ms = f'{cy:04d}{cm:02d}01' if (cy, cm) != (sy, sm) else start_yyyymmdd
            me_d = mlast if (cy, cm) != (ey, em) else min(mlast, int(end_yyyymmdd[6:8]))
            me = f'{cy:04d}{cm:02d}{me_d:02d}'
            months.append((f'{cy:04d}-{cm:02d}', ms, me))
            cm += 1
            if cm > 12: cy += 1; cm = 1

        log(f'─ 월별 합계 ({len(months)} 개월) ─')
        log(f'{"월":>8} {"agro(억)":>11} {"우리(억)":>11} {"비율%":>7} {"차이(억)":>11} {"missing":>7} {"orphan":>7}')
        log('─' * 78)
        for i, (label, ms, me) in enumerate(months):
            if stop_cb and stop_cb():
                log('⏹ 중단'); break
            try:
                m = compare_period(ms, me, agro_root=agro_root, threshold_pct=threshold_pct)
            except Exception as ex:
                log(f'  ⚠ {label} 실패: {ex}'); continue
            mt = m['totals']; mfc = m['flag_counts']
            mark = '⚠ ' if abs(100 - mt['amt_ratio_pct']) >= threshold_pct and mt['remote_amt'] > 0 else '  '
            log(f'{mark}{label:>6} {mt["remote_amt"]/1e8:>10,.2f} {mt["local_amt"]/1e8:>10,.2f} '
                f'{mt["amt_ratio_pct"]:>7.2f} {mt["amt_diff"]/1e8:>+10,.2f} '
                f'{mfc["missing"]:>7} {mfc["orphan"]:>7}')
            monthly.append({'label': label, 'totals': mt, 'flag_counts': mfc,
                            'period': m['period']})
            if progress_cb:
                try: progress_cb(i+1, len(months), label)
                except: pass

    finished = datetime.now()
    elapsed = (finished - started).total_seconds()
    log('')
    log('=' * 70)
    log(f'🎯 검수 완료 — 소요 {elapsed:.1f}초')

    return {
        'period_total':  period_total,
        'monthly':       monthly,
        'started_at':    started.isoformat(),
        'finished_at':   finished.isoformat(),
        'elapsed_sec':   elapsed,
        'agro_root':     agro_root,
        'threshold_pct': threshold_pct,
        'with_monthly':  with_monthly_breakdown,
    }


# ── CLI ──
if __name__ == '__main__':
    import sys, argparse, json
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description='agroverify CLI — (시장, 법인) 단위 비교')
    ap.add_argument('start', help='YYYYMMDD')
    ap.add_argument('end',   help='YYYYMMDD')
    ap.add_argument('--agro-root', default=DEFAULT_AGRO_ROOT)
    ap.add_argument('--no-monthly', action='store_true', help='월별 분해 끄기')
    ap.add_argument('--threshold', type=float, default=5.0)
    ap.add_argument('--top-n', type=int, default=30, help='TOP N (시장,법인) 출력')
    ap.add_argument('--output', help='결과 JSON 저장 경로')
    args = ap.parse_args()

    res = run_verify(args.start, args.end,
                     agro_root=args.agro_root,
                     with_monthly_breakdown=not args.no_monthly,
                     threshold_pct=args.threshold,
                     top_n_rows=args.top_n,
                     log_cb=print)
    if args.output:
        out_p = os.path.abspath(args.output)
        os.makedirs(os.path.dirname(out_p), exist_ok=True)
        with open(out_p, 'w', encoding='utf-8') as f:
            json.dump(res, f, ensure_ascii=False, indent=2, default=str)
        print(f'\n💾 결과 저장: {out_p}')
