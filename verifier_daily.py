"""
verifier_daily.py — 일자×(시장,법인) 단위 검증 + 부족 페어만 재다운
==================================================================

설계 v0.5 (사용자 명시 2026-05-02):
  - 365일 모두 fetch (일요일 포함 — 일부 일요일 거래 있음)
  - 일자 단위 검증 (월 합산은 작은 누락 가려져)
  - 시장별 일치율 다르면 그 시장의 그 일자만 재다운

기존 verifier_core.compare_period 활용. 단, 영업일 정의 변경:
  - _iter_calendar_days: weekday 무관, 모든 날짜 yield
  - compare_period(ymd, ymd) 를 일자별 호출

CLI:
  # 일자×시장 진단 (사용자 표본용)
  python verifier_daily.py diag 20240701 20240731 --output diag_2024_07.json

  # 일자×시장 단위 자동 fix
  python verifier_daily.py auto-fix 201601 202604 --output autofix_daily.json
"""
from __future__ import annotations
import os, sys, json, sqlite3, time
from datetime import date, timedelta
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from verifier_core import compare_period, fetch_remote_period_corp, DEFAULT_AGRO_ROOT


def iter_calendar_days(start_yyyymmdd: str, end_yyyymmdd: str):
    """일요일 포함 모든 날짜 yield."""
    sy, sm, sd = int(start_yyyymmdd[:4]), int(start_yyyymmdd[4:6]), int(start_yyyymmdd[6:8])
    ey, em, ed = int(end_yyyymmdd[:4]),   int(end_yyyymmdd[4:6]),   int(end_yyyymmdd[6:8])
    d = date(sy, sm, sd)
    end = date(ey, em, ed)
    while d <= end:
        yield d.strftime('%Y%m%d'), d.weekday()
        d += timedelta(days=1)


def db_file_for(ymd: str, agro_root: str = DEFAULT_AGRO_ROOT) -> str:
    return os.path.join(agro_root, 'daily', ymd[:4], f'agro_{ymd}.db')


def db_row_count(p: str) -> int:
    if not os.path.isfile(p): return 0
    try:
        dc = sqlite3.connect(f'file:{p}?mode=ro', uri=True)
        n = dc.execute('SELECT COUNT(*) FROM agro_trades').fetchone()[0]
        dc.close()
        return n
    except Exception:
        return -1


WD_KOR = ['월','화','수','목','금','토','일']


def diag_daily_per_market(start_yyyymmdd: str, end_yyyymmdd: str,
                           agro_root: str = DEFAULT_AGRO_ROOT,
                           threshold_pct: float = 5.0,
                           log_cb=None) -> dict:
    """일자 단위 검증 — agromarket vs 우리 DB.

    매 일자 (시장,법인) 단위 비교 후:
      - 일자별 합계
      - 시장별 일치율 (전 일자 합산)
      - 부족 (시장, 일자) 페어 list
      - flag count

    Returns: dict with 'days', 'by_market', 'shortage_pairs', 'totals'
    """
    log = log_cb or print

    days_data = []
    market_total = defaultdict(lambda: {'r_amt': 0.0, 'l_amt': 0.0,
                                         'r_qty': 0.0, 'l_qty': 0.0,
                                         'days_short': set(), 'days_over': set(),
                                         'days_ok': set(), 'days_missing': set()})
    shortage_pairs = []   # [(market, ymd, r_amt, l_amt, ratio, flag)]
    over_pairs = []        # 우리 DB > agromarket 5%↑ (중복 적재 의심)
    missing_pairs = []     # agromarket 에만 (우리 DB 0)
    sun_with_data = []     # 일요일인데 거래 있는 날
    no_file_days = []      # 파일 자체 없는 일자 (휴장 또는 미다운)

    sum_r_amt = sum_l_amt = 0.0
    sum_r_qty = sum_l_qty = 0.0

    for ymd, wd in iter_calendar_days(start_yyyymmdd, end_yyyymmdd):
        wd_k = WD_KOR[wd]
        p = db_file_for(ymd, agro_root)
        has_file = os.path.isfile(p)
        nrow = db_row_count(p)

        try:
            cmp = compare_period(ymd, ymd, agro_root=agro_root, threshold_pct=threshold_pct)
        except Exception as ex:
            log(f'  ⚠ {ymd} 비교 실패: {ex}')
            continue

        t = cmp['totals']
        r_amt = t['remote_amt']
        l_amt = t['local_amt']
        r_qty = t['remote_qty']
        l_qty = t['local_qty']
        amt_pct = (l_amt / r_amt * 100) if r_amt > 0 else 0
        n_keys = len(cmp['rows'])

        sum_r_amt += r_amt; sum_l_amt += l_amt
        sum_r_qty += r_qty; sum_l_qty += l_qty

        # 일요일에 거래 있나
        if wd == 6 and r_amt > 0:
            sun_with_data.append((ymd, r_amt))

        if not has_file and r_amt > 0:
            no_file_days.append((ymd, wd_k, r_amt))

        # 시장별 합산 (그 일자 row 들 → 시장 → 합)
        by_market_today = defaultdict(lambda: {'r_amt': 0.0, 'l_amt': 0.0,
                                                  'r_qty': 0.0, 'l_qty': 0.0})
        for row in cmp['rows']:
            mk = row['market']
            by_market_today[mk]['r_amt'] += row['remote_amt']
            by_market_today[mk]['l_amt'] += row['local_amt']
            by_market_today[mk]['r_qty'] += row['remote_qty']
            by_market_today[mk]['l_qty'] += row['local_qty']

        # 시장별 (그 일자) flag 판정
        for mk, v in by_market_today.items():
            row_data = {
                'market': mk, 'date': ymd, 'weekday': wd_k,
                'remote_amt': v['r_amt'], 'local_amt': v['l_amt'],
                'remote_qty': v['r_qty'], 'local_qty': v['l_qty'],
                'amt_pct': (v['l_amt']/v['r_amt']*100) if v['r_amt'] > 0 else 0,
                'amt_diff': v['l_amt'] - v['r_amt'],
            }
            if v['r_amt'] > 0 and v['l_amt'] == 0:
                # missing — agromarket 있는데 우리 없음 (재다운 필요)
                row_data['flag'] = 'missing'
                missing_pairs.append(row_data)
                market_total[mk]['days_missing'].add(ymd)
            elif v['r_amt'] > 0:
                ratio = v['l_amt'] / v['r_amt'] * 100
                if ratio < (100 - threshold_pct):
                    row_data['flag'] = 'short'
                    shortage_pairs.append(row_data)
                    market_total[mk]['days_short'].add(ymd)
                elif ratio > (100 + threshold_pct):
                    row_data['flag'] = 'over'
                    over_pairs.append(row_data)
                    market_total[mk]['days_over'].add(ymd)
                else:
                    row_data['flag'] = 'ok'
                    market_total[mk]['days_ok'].add(ymd)
            elif v['l_amt'] > 0:
                # orphan — agro=0 인데 우리 있음 (시장이 없거나 거짓 적재)
                pass
            market_total[mk]['r_amt'] += v['r_amt']
            market_total[mk]['l_amt'] += v['l_amt']
            market_total[mk]['r_qty'] += v['r_qty']
            market_total[mk]['l_qty'] += v['l_qty']

        days_data.append({
            'date': ymd, 'weekday': wd_k, 'has_file': has_file, 'db_rows': nrow,
            'remote_amt': r_amt, 'local_amt': l_amt,
            'remote_qty': r_qty, 'local_qty': l_qty,
            'amt_pct': amt_pct, 'n_keys': n_keys,
        })

    # 시장별 합산 → 일치율
    market_summary = []
    for mk, v in market_total.items():
        if v['r_amt'] > 0:
            mratio = v['l_amt'] / v['r_amt'] * 100
        else:
            mratio = 0
        market_summary.append({
            'market': mk,
            'remote_amt': v['r_amt'],
            'local_amt': v['l_amt'],
            'amt_pct': mratio,
            'amt_diff': v['l_amt'] - v['r_amt'],
            'days_ok': len(v['days_ok']),
            'days_short': len(v['days_short']),
            'days_over': len(v['days_over']),
            'days_missing': len(v['days_missing']),
        })
    market_summary.sort(key=lambda x: -abs(x['amt_diff']))

    return {
        'period': f'{start_yyyymmdd}~{end_yyyymmdd}',
        'totals': {
            'remote_amt': sum_r_amt, 'local_amt': sum_l_amt,
            'remote_qty': sum_r_qty, 'local_qty': sum_l_qty,
            'amt_pct': (sum_l_amt/sum_r_amt*100) if sum_r_amt else 0,
            'qty_pct': (sum_l_qty/sum_r_qty*100) if sum_r_qty else 0,
            'n_short_pairs': len(shortage_pairs),
            'n_over_pairs': len(over_pairs),
            'n_missing_pairs': len(missing_pairs),
        },
        'days': days_data,
        'by_market': market_summary,
        'shortage_pairs': shortage_pairs,
        'over_pairs': over_pairs,
        'missing_pairs': missing_pairs,
        'sundays_with_data': sun_with_data,
        'no_file_with_remote': no_file_days,
    }


def fmt_diag(res: dict) -> str:
    """진단 결과 사람이 읽기 좋게 출력."""
    lines = []
    t = res['totals']
    lines.append(f"📊 진단 — {res['period']}")
    lines.append(f"  합계 amount : agromarket {t['remote_amt']/1e8:>12,.2f} 억 vs 우리 {t['local_amt']/1e8:>12,.2f} 억 → {t['amt_pct']:.2f}%")
    lines.append(f"  합계 qty    : agromarket {t['remote_qty']/1000:>12,.0f} 톤 vs 우리 {t['local_qty']/1000:>12,.0f} 톤 → {t['qty_pct']:.2f}%")
    lines.append('')
    lines.append(f"⬇ 부족(short) (시장×일자): {len(res['shortage_pairs'])}")
    lines.append(f"⬆ 과다(over)  (시장×일자): {len(res['over_pairs'])}  ← 중복 적재 의심")
    lines.append(f"❌ 누락(missing) (시장×일자): {len(res['missing_pairs'])}  ← 재다운 필요")
    lines.append(f"   일요일인데 거래 있는 날: {len(res['sundays_with_data'])}")
    if res['sundays_with_data'][:10]:
        lines.append(f"     예: {[d for d, _ in res['sundays_with_data'][:10]]}")
    lines.append(f"   파일 없는데 agromarket 거래 있는 날: {len(res['no_file_with_remote'])}")
    if res['no_file_with_remote'][:10]:
        for d, wd, ra in res['no_file_with_remote'][:10]:
            lines.append(f"     - {d}({wd}) agro={ra/1e8:,.2f}억")
    lines.append('')

    # 일자 표
    lines.append('─ 일자 단위 ─')
    lines.append(f"{'날짜':>12} {'요일':>3} {'agro(억)':>11} {'우리(억)':>11} {'amt%':>6} {'agroQty':>10} {'우리Qty':>10} {'qty%':>6} {'파일':>3} {'행':>7}")
    lines.append('─' * 100)
    for d in res['days']:
        tag = ''
        if not d['has_file'] and d['remote_amt'] > 0: tag = ' ❌NOFILE'
        elif d['amt_pct'] >= 95: tag = ' ✅'
        elif d['remote_amt'] == 0: tag = ' (휴장)'
        else: tag = f' ⬇{100-d["amt_pct"]:.0f}%'
        lines.append(f"{d['date'][:4]}-{d['date'][4:6]}-{d['date'][6:]} {d['weekday']:>3} "
                     f"{d['remote_amt']/1e8:>10,.2f} {d['local_amt']/1e8:>10,.2f} "
                     f"{d['amt_pct']:>5.1f}% "
                     f"{d['remote_qty']/1000:>10,.0f} {d['local_qty']/1000:>10,.0f} "
                     f"{(d['local_qty']/d['remote_qty']*100) if d['remote_qty'] > 0 else 0:>5.1f}% "
                     f"{'O' if d['has_file'] else 'X':>3} {d['db_rows']:>7,}{tag}")
    lines.append('')

    # 시장 표 (TOP 부족)
    lines.append('─ 시장별 합계 (전 기간) ─')
    lines.append(f"{'시장':<14} {'agro(억)':>11} {'우리(억)':>11} {'amt%':>6} {'차이(억)':>11} {'OK일':>5} {'부족일':>6}")
    lines.append('─' * 75)
    for m in res['by_market']:
        lines.append(f"{m['market'][:14]:<14} {m['remote_amt']/1e8:>10,.2f} {m['local_amt']/1e8:>10,.2f} "
                     f"{m['amt_pct']:>5.1f}% {m['amt_diff']/1e8:>+10,.2f} "
                     f"{m['days_ok']:>5} {m['days_short']:>6}")
    lines.append('')

    # 부족 페어 TOP 30
    if res['shortage_pairs']:
        lines.append(f"─ 부족 (시장×일자) TOP 30 (금액 차이 큰 순) ─")
        lines.append(f"{'시장':<14} {'일자':>10} {'요일':>3} {'agro(억)':>10} {'우리(억)':>10} {'amt%':>6} {'차이(억)':>11}")
        lines.append('─' * 80)
        sp = sorted(res['shortage_pairs'], key=lambda x: -abs(x['amt_diff']))[:30]
        for s in sp:
            lines.append(f"{s['market'][:14]:<14} {s['date'][:4]}-{s['date'][4:6]}-{s['date'][6:]:>2} "
                         f"{s['weekday']:>3} {s['remote_amt']/1e8:>9,.2f} {s['local_amt']/1e8:>9,.2f} "
                         f"{s['amt_pct']:>5.1f}% {s['amt_diff']/1e8:>+10,.2f}")

    return '\n'.join(lines)


def main():
    if len(sys.argv) < 4:
        print('Usage:')
        print('  python verifier_daily.py diag YYYYMMDD YYYYMMDD [--output path]')
        print('  python verifier_daily.py auto-fix YYYYMM YYYYMM [--output path]   # not yet')
        return 1

    sys.stdout.reconfigure(encoding='utf-8')
    cmd = sys.argv[1]
    if cmd == 'diag':
        start, end = sys.argv[2], sys.argv[3]
        out_path = None
        if '--output' in sys.argv:
            out_path = sys.argv[sys.argv.index('--output') + 1]
        t0 = time.time()
        res = diag_daily_per_market(start, end)
        elapsed = time.time() - t0
        print(fmt_diag(res))
        print(f'\n⏱ 소요 {elapsed:.1f}초')
        if out_path:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(res, f, ensure_ascii=False, indent=2, default=str)
            print(f'💾 저장: {out_path}')
        return 0
    print(f'unknown cmd: {cmd}')
    return 1


if __name__ == '__main__':
    sys.exit(main())
