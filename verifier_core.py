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


# ── v0.4: auto_fix — 검증+자동 재다운+재적재 cycle ──
def _import_innong():
    """innong 의 agro/agro_db/markets.json 확보. apps/innong 가 형제 폴더라고 가정."""
    import sys
    here = os.path.dirname(os.path.abspath(__file__))
    innong_path = os.path.normpath(os.path.join(here, '..', 'innong'))
    if innong_path not in sys.path:
        sys.path.insert(0, innong_path)
    import agro as innong_agro       # noqa: E402
    import agro_db as innong_agro_db # noqa: E402
    return innong_agro, innong_agro_db, innong_path


def _load_market_map(innong_path: str) -> dict:
    """markets.json (시장명 → 시장코드) 매핑."""
    import json as _json
    p = os.path.join(innong_path, 'markets.json')
    with open(p, encoding='utf-8') as f:
        markets = _json.load(f)
    return {m['name']: m['code'] for m in markets}


def auto_fix(start_ym: str = '201901', end_ym: str = '202604',
             agro_root: str = DEFAULT_AGRO_ROOT,
             threshold_pct: float = 5.0,
             latest_first: bool = True,
             dry_run: bool = False,
             only_market_codes: list = None,
             skip_market_codes: list = None,
             download_unverifiable: bool = True,
             log_cb=None,
             stop_cb=None) -> dict:
    """v0.4: 자동 검증 + 부족 시 재다운 + 재적재.

    Args:
      start_ym, end_ym: 'YYYYMM'
      threshold_pct: 부족 판정 임계 (default 5%)
      latest_first: 최신 월부터 역순 (default True)
      dry_run: True 면 다운로드/적재 없이 부족 list 만 출력
      download_unverifiable: agromarket 응답 0인 월도 무조건 다운 시도 (사용자 요청)

    Flow per month:
      1) compare_period 호출 → (시장,법인) 단위 비교
      2) 시장 단위 합산 → 부족 시장 list (또는 amount=0 면 검증 불가)
      3) 검증 가능 + 부족 → 재다운 + 재적재
      4) 검증 불가 + download_unverifiable → 모든 시장 다운 시도 (스킵 인 daily DB 존재 일자 등)
      5) 결과 누적
    """
    log = log_cb or print
    from datetime import datetime
    from calendar import monthrange
    from collections import defaultdict
    started = datetime.now()

    # innong import
    try:
        innong_agro, innong_agro_db, innong_path = _import_innong()
        market_map = _load_market_map(innong_path)
    except Exception as ex:
        log(f'❌ innong 모듈/markets.json 로드 실패: {ex}')
        log(f'   innong 이 apps/innong 형제 폴더에 있어야 함')
        return {'error': str(ex)}

    log(f'🚀 auto_fix 시작 — {start_ym}~{end_ym}, '
        f'{"최신순" if latest_first else "과거순"}, 임계 {threshold_pct}%')
    log(f'   {"[DRY-RUN] 실제 다운/적재 X" if dry_run else "[REAL] 실제 진행"}')
    log(f'   검증 불가 월 다운: {"ON" if download_unverifiable else "OFF"}')
    log(f'   markets.json: {len(market_map)} 시장')
    if only_market_codes:
        log(f'   only_market_codes: {only_market_codes}')
    if skip_market_codes:
        log(f'   skip_market_codes: {skip_market_codes}')

    # 월 list
    sy, sm = int(start_ym[:4]), int(start_ym[4:6])
    ey, em = int(end_ym[:4]),   int(end_ym[4:6])
    months = []
    cy, cm = sy, sm
    while (cy, cm) <= (ey, em):
        ms = f'{cy:04d}{cm:02d}01'
        me = f'{cy:04d}{cm:02d}{monthrange(cy, cm)[1]:02d}'
        months.append((f'{cy:04d}-{cm:02d}', ms, me))
        cm += 1
        if cm > 12: cy += 1; cm = 1
    if latest_first:
        months.reverse()
    log(f'   대상: {len(months)} 개월')

    downloader = None  # lazy init

    summary = {
        'start_ym': start_ym, 'end_ym': end_ym,
        'months_total': len(months),
        'months_processed': 0,
        'months_verifiable': 0,
        'months_unverifiable': 0,
        'months_with_shortage': 0,
        'shortage_market_pairs': 0,
        'redownloads_attempted': 0,
        'redownloads_failed': 0,
        'reload_files': 0, 'reload_rows': 0,
        'months_detail': [],
        'started_at': started.isoformat(),
        'dry_run': dry_run,
    }

    for i, (label, ms, me) in enumerate(months):
        if stop_cb and stop_cb():
            log('⏹ 사용자 중단'); break
        log(f'\n━━ [{i+1}/{len(months)}] {label} ━━')

        # 1) 비교
        try:
            cmp = compare_period(ms, me, agro_root=agro_root, threshold_pct=threshold_pct)
        except Exception as ex:
            log(f'  ⚠ 비교 실패: {ex}')
            continue

        # 2) 시장 단위 합산
        by_market = defaultdict(lambda: {'r_amt': 0.0, 'l_amt': 0.0})
        for d in cmp['rows']:
            mk = d['market']
            by_market[mk]['r_amt'] += d['remote_amt']
            by_market[mk]['l_amt'] += d['local_amt']

        total_remote = sum(v['r_amt'] for v in by_market.values())
        verifiable = total_remote > 0
        if verifiable:
            summary['months_verifiable'] += 1
        else:
            summary['months_unverifiable'] += 1

        # 3) 부족 시장 또는 검증 불가 시 처리 대상 결정
        targets = []   # [(market_name, market_code, reason), ...]
        if verifiable:
            for mk, v in by_market.items():
                if v['r_amt'] <= 0:
                    continue
                ratio = v['l_amt'] / v['r_amt'] * 100
                if ratio < (100 - threshold_pct):
                    mc = market_map.get(mk)
                    if mc:
                        targets.append((mk, mc, f'shortage {ratio:.1f}%'))
                    else:
                        log(f'    ⚠ {mk} → markets.json 매핑 없음, skip')
        elif download_unverifiable:
            # 모든 시장 다운 시도
            for mk, mc in market_map.items():
                targets.append((mk, mc, 'unverifiable, force download'))
            log(f'  검증 불가 월 → {len(targets)} 시장 다운 시도')

        # 시장 필터
        if only_market_codes:
            targets = [t for t in targets if t[1] in only_market_codes]
        if skip_market_codes:
            targets = [t for t in targets if t[1] not in skip_market_codes]

        if verifiable and not targets:
            log(f'  ✅ 검증 통과 (부족 시장 없음, 시장 {len(by_market)}, 합계 비율 '
                f'{cmp["totals"]["amt_ratio_pct"]:.1f}%)')
            summary['months_processed'] += 1
            summary['months_detail'].append({'label': label, 'verifiable': True,
                                              'shortage_count': 0,
                                              'amt_ratio_pct': cmp['totals']['amt_ratio_pct']})
            continue

        if targets:
            summary['months_with_shortage'] += 1 if verifiable else 0
            summary['shortage_market_pairs'] += len(targets)
            log(f'  📋 처리 대상 {len(targets)} 시장:')
            for mk, mc, reason in targets[:8]:
                log(f'    - [{mc}] {mk}  ({reason})')
            if len(targets) > 8:
                log(f'    - ... 외 {len(targets)-8}')

        if dry_run:
            log(f'  [DRY-RUN] 실제 다운/적재 skip')
            summary['months_processed'] += 1
            summary['months_detail'].append({'label': label, 'verifiable': verifiable,
                                              'shortage_count': len(targets), 'dry_run': True})
            continue

        # 4) 재다운 + 재적재
        if downloader is None:
            log(f'  🌐 AgroDownloader 초기화 (Selenium Chrome)')
            downloader = innong_agro.AgroDownloader(log_callback=log)

        ms_dash = f'{ms[:4]}-{ms[4:6]}-{ms[6:]}'
        me_dash = f'{me[:4]}-{me[4:6]}-{me[6:]}'

        for mk, mc, reason in targets:
            if stop_cb and stop_cb():
                log('⏹ 중단'); break
            log(f'  🔽 [{mc}] {mk} ({reason}) 재다운 중...')
            files_received = []
            def _cb(payload):
                # payload = (xls_path, m_code, m_name)
                files_received.append(payload)
            try:
                downloader.run_download_daily(
                    start_date_str=ms_dash, end_date_str=me_dash,
                    selected_codes=[mc],
                    file_ready_cb=_cb,
                    force_redownload=True,
                )
                summary['redownloads_attempted'] += 1
            except Exception as ex:
                log(f'    ❌ 다운 실패: {ex}')
                summary['redownloads_failed'] += 1
                continue

            log(f'    📂 받은 XLS {len(files_received)} 개, 적재 중...')
            for (xls_path, m_code, m_name) in files_received:
                try:
                    n = innong_agro_db.load_file_single(
                        xls_path, m_code, m_name,
                        force=True, logger=lambda s: None)
                    if n > 0:
                        summary['reload_files'] += 1
                        summary['reload_rows'] += n
                except Exception as ex:
                    log(f'      ⚠ 적재 실패 {os.path.basename(xls_path)}: {ex}')

        # 5) 재검증
        try:
            cmp2 = compare_period(ms, me, agro_root=agro_root, threshold_pct=threshold_pct)
            log(f'  ↻ 재검증: 비율 {cmp2["totals"]["amt_ratio_pct"]:.1f}% '
                f'(이전 {cmp["totals"]["amt_ratio_pct"]:.1f}%)')
            summary['months_detail'].append({
                'label': label, 'verifiable': verifiable,
                'shortage_count': len(targets),
                'before_ratio': cmp['totals']['amt_ratio_pct'],
                'after_ratio':  cmp2['totals']['amt_ratio_pct'],
            })
        except Exception:
            pass
        summary['months_processed'] += 1

    finished = datetime.now()
    summary['finished_at'] = finished.isoformat()
    summary['elapsed_sec'] = (finished - started).total_seconds()
    log('')
    log('=' * 70)
    log(f'🎯 auto_fix 완료 — 소요 {summary["elapsed_sec"]:.0f}초')
    log(f'   처리 월: {summary["months_processed"]}/{summary["months_total"]}')
    log(f'   검증 가능 {summary["months_verifiable"]} / 불가 {summary["months_unverifiable"]}')
    log(f'   부족 월 {summary["months_with_shortage"]} '
        f'(시장 누적 {summary["shortage_market_pairs"]})')
    log(f'   재다운 {summary["redownloads_attempted"]} '
        f'(실패 {summary["redownloads_failed"]})')
    log(f'   적재 파일 {summary["reload_files"]}, row {summary["reload_rows"]:,}')
    return summary


# ── CLI ──
if __name__ == '__main__':
    import sys, argparse, json
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description='agroverify CLI')
    sub = ap.add_subparsers(dest='cmd', required=False)

    # 기본: verify (이전 동작)
    p_v = sub.add_parser('verify', help='기간 검수만')
    p_v.add_argument('start', help='YYYYMMDD')
    p_v.add_argument('end',   help='YYYYMMDD')
    p_v.add_argument('--agro-root', default=DEFAULT_AGRO_ROOT)
    p_v.add_argument('--no-monthly', action='store_true')
    p_v.add_argument('--threshold', type=float, default=5.0)
    p_v.add_argument('--top-n', type=int, default=30)
    p_v.add_argument('--output')

    # auto-fix: 검증 + 재다운 + 재적재
    p_a = sub.add_parser('auto-fix', help='자동 검증+재다운+재적재 cycle')
    p_a.add_argument('start_ym', help='YYYYMM')
    p_a.add_argument('end_ym',   help='YYYYMM')
    p_a.add_argument('--agro-root', default=DEFAULT_AGRO_ROOT)
    p_a.add_argument('--threshold', type=float, default=5.0)
    p_a.add_argument('--oldest-first', action='store_true', help='과거 → 최신 (기본은 최신 → 과거)')
    p_a.add_argument('--dry-run', action='store_true')
    p_a.add_argument('--only-markets', nargs='+', help='특정 시장 코드만')
    p_a.add_argument('--skip-markets', nargs='+', help='특정 시장 코드 제외')
    p_a.add_argument('--no-download-unverifiable', action='store_true',
                     help='검증 불가 월 다운 시도 끄기 (default ON)')
    p_a.add_argument('--output', help='결과 JSON 저장 경로')

    args = ap.parse_args()

    if args.cmd == 'auto-fix':
        res = auto_fix(args.start_ym, args.end_ym,
                       agro_root=args.agro_root,
                       threshold_pct=args.threshold,
                       latest_first=not args.oldest_first,
                       dry_run=args.dry_run,
                       only_market_codes=args.only_markets,
                       skip_market_codes=args.skip_markets,
                       download_unverifiable=not args.no_download_unverifiable,
                       log_cb=print)
    else:
        # default: verify
        if args.cmd != 'verify':
            # backward compat: 옛 호출 그대로 (positional start end)
            args = ap.parse_args(['verify'] + sys.argv[1:])
        res = run_verify(args.start, args.end,
                         agro_root=args.agro_root,
                         with_monthly_breakdown=not args.no_monthly,
                         threshold_pct=args.threshold,
                         top_n_rows=args.top_n,
                         log_cb=print)

    if getattr(args, 'output', None):
        out_p = os.path.abspath(args.output)
        os.makedirs(os.path.dirname(out_p), exist_ok=True)
        with open(out_p, 'w', encoding='utf-8') as f:
            json.dump(res, f, ensure_ascii=False, indent=2, default=str)
        print(f'\n💾 결과 저장: {out_p}')
