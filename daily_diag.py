"""
daily_diag.py — 일자 단위 일치율 진단
=====================================

기간 받아서 매 영업일별 (agromarket vs 우리 DB) 합계 비교 표.
어느 일자가 통째 누락 / 부분 부족 / 일치인지 한눈에 파악.

Usage:
  python daily_diag.py 20250101 20250131
  python daily_diag.py 20250101 20250131 --output diag_2025_01_daily.json

출력 컬럼:
  날짜  agro(억)  우리(억)  비율%  amt차이(억)  agro물량  우리물량  qty비율  파일
"""
from __future__ import annotations
import os, sys, json, sqlite3
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from verifier_core import compare_period, _iter_business_days, DEFAULT_AGRO_ROOT

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

def main():
    if len(sys.argv) < 3:
        print('Usage: python daily_diag.py YYYYMMDD YYYYMMDD [--output path]')
        return 1
    start, end = sys.argv[1], sys.argv[2]
    out_path = None
    if '--output' in sys.argv:
        out_path = sys.argv[sys.argv.index('--output') + 1]

    sys.stdout.reconfigure(encoding='utf-8')
    print(f'🔍 일자 단위 진단 — {start} ~ {end}')
    print()
    print(f'{"날짜":>10} {"agro(억)":>10} {"우리(억)":>10} {"amt%":>7} {"diff(억)":>10} '
          f'{"agroQty":>10} {"우리Qty":>10} {"qty%":>7} {"파일":>4} {"행":>7} {"key수":>5} {"플래그":>5}')
    print('─' * 130)

    rows = []
    sum_r_amt = sum_l_amt = 0.0
    sum_r_qty = sum_l_qty = 0.0
    n_no_file = n_zero_remote = n_full = n_partial = n_empty = 0

    for ymd in _iter_business_days(start, end):
        p = db_file_for(ymd)
        has = os.path.isfile(p)
        nrow = db_row_count(p)

        try:
            cmp = compare_period(ymd, ymd, threshold_pct=5)
            t = cmp['totals']
            fc = cmp['flag_counts']
            r_amt = t['remote_amt']
            l_amt = t['local_amt']
            r_qty = t['remote_qty']
            l_qty = t['local_qty']
        except Exception as ex:
            print(f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}  ERROR: {ex}')
            continue

        sum_r_amt += r_amt; sum_l_amt += l_amt
        sum_r_qty += r_qty; sum_l_qty += l_qty
        amt_pct = (l_amt / r_amt * 100) if r_amt > 0 else 0
        qty_pct = (l_qty / r_qty * 100) if r_qty > 0 else 0
        n_keys = len(cmp['rows'])

        # 분류
        if not has:
            tag = 'NOFILE'; n_no_file += 1
        elif r_amt <= 0:
            tag = 'AGRO=0'; n_zero_remote += 1
        elif l_amt <= 0:
            tag = 'EMPTY'; n_empty += 1
        elif abs(amt_pct - 100) < 5:
            tag = 'OK'; n_full += 1
        else:
            tag = 'SHORT'; n_partial += 1

        print(f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]} '
              f'{r_amt/1e8:>10,.2f} {l_amt/1e8:>10,.2f} '
              f'{amt_pct:>6.1f}% {(l_amt-r_amt)/1e8:>+10,.2f} '
              f'{r_qty/1000:>10,.0f} {l_qty/1000:>10,.0f} '
              f'{qty_pct:>6.1f}% {"O" if has else "X":>4} '
              f'{nrow:>7,} {n_keys:>5} {tag:>6}')
        rows.append({
            'date': ymd, 'has_file': has, 'db_rows': nrow,
            'remote_amt': r_amt, 'local_amt': l_amt,
            'remote_qty': r_qty, 'local_qty': l_qty,
            'amt_pct': amt_pct, 'qty_pct': qty_pct,
            'n_keys': n_keys, 'tag': tag,
        })

    print('─' * 130)
    tot_amt_pct = (sum_l_amt / sum_r_amt * 100) if sum_r_amt else 0
    tot_qty_pct = (sum_l_qty / sum_r_qty * 100) if sum_r_qty else 0
    print(f'{"합계":>10} {sum_r_amt/1e8:>10,.2f} {sum_l_amt/1e8:>10,.2f} '
          f'{tot_amt_pct:>6.1f}% {(sum_l_amt-sum_r_amt)/1e8:>+10,.2f} '
          f'{sum_r_qty/1000:>10,.0f} {sum_l_qty/1000:>10,.0f} '
          f'{tot_qty_pct:>6.1f}%')
    print()
    print(f'분류: OK={n_full}  SHORT={n_partial}  EMPTY(우리DB비어)={n_empty}  '
          f'NOFILE(파일없음)={n_no_file}  AGRO=0(원본0)={n_zero_remote}')

    if out_path:
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump({
                'period': f'{start}~{end}',
                'totals': {
                    'remote_amt': sum_r_amt, 'local_amt': sum_l_amt,
                    'remote_qty': sum_r_qty, 'local_qty': sum_l_qty,
                    'amt_pct': tot_amt_pct, 'qty_pct': tot_qty_pct,
                },
                'tags': {
                    'OK': n_full, 'SHORT': n_partial, 'EMPTY': n_empty,
                    'NOFILE': n_no_file, 'AGRO_ZERO': n_zero_remote,
                },
                'days': rows,
            }, f, ensure_ascii=False, indent=2, default=str)
        print(f'\n💾 저장: {out_path}')

    return 0

if __name__ == '__main__':
    sys.exit(main())
