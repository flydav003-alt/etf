"""
fetch_holdings.py — 台股主動式ETF持股抓取系統
資料來源：Pocket.tw M722 API（持股）
         TWSE OpenAPI（個股收盤價、ETF價格、規模）
"""

import requests
import sqlite3
import pandas as pd
import json
import time
import logging
from datetime import date, timedelta
from pathlib import Path

# ══════════════════════════════════════════════════════════
# logging
# ══════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('etf_tracker.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# ★ 只需改這裡就能新增／移除追蹤的 ETF ★
# ══════════════════════════════════════════════════════════
ACTIVE_ETFS = {
    '00981A': '統一台股增長',
    '00982A': '群益台灣強棒',
    '00991A': '復華未來50',
    '00992A': '群益科技創新',
    '00993A': '安聯台灣',
    '00980A': '野村臺灣優選',
    '00985A': '野村台灣50',
    '00995A': '中信台灣卓越',
    '00984A': '安聯台灣高息',
    '00987A': '台新優勢成長',
    '00994A': '第一金台股優',
    '00996A': '兆豐台灣豐收',
    '00400A': '國泰動能高息',
    '00401A': '摩根台灣鑫收',
}

# 績效基準（不做持股比對，只抓價格）
BENCHMARK_ETFS = {
    '0050':   '元大台灣50',
    '009816': '凱基台灣TOP50',
}

# 全部要抓價格的 ETF（主動 + 基準）
ALL_PRICE_ETFS = {**ACTIVE_ETFS, **BENCHMARK_ETFS}

DB_PATH  = 'etf_tracker.db'
DATA_DIR = Path('data')
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
    'Referer':    'https://www.pocket.tw/etf/tw/00981A/fundholding',
}


# ══════════════════════════════════════════════════════════
# 1. 資料庫初始化
# ══════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS daily_holdings (
        trade_date  TEXT NOT NULL,
        etf_code    TEXT NOT NULL,
        stock_code  TEXT NOT NULL,
        stock_name  TEXT,
        weight_pct  REAL DEFAULT 0,
        shares      REAL DEFAULT 0,
        close_price REAL DEFAULT 0,
        amount_est  REAL DEFAULT 0,
        PRIMARY KEY (trade_date, etf_code, stock_code)
    );

    CREATE TABLE IF NOT EXISTS holdings_changes (
        trade_date    TEXT NOT NULL,
        etf_code      TEXT NOT NULL,
        stock_code    TEXT NOT NULL,
        stock_name    TEXT,
        action        TEXT NOT NULL,
        weight_before REAL DEFAULT 0,
        weight_after  REAL DEFAULT 0,
        weight_change REAL DEFAULT 0,
        shares_change REAL DEFAULT 0,
        close_price   REAL DEFAULT 0,
        amount_change REAL DEFAULT 0,
        PRIMARY KEY (trade_date, etf_code, stock_code)
    );

    -- ETF 每日收盤價（主動ETF + 基準ETF）
    CREATE TABLE IF NOT EXISTS etf_prices (
        trade_date  TEXT NOT NULL,
        etf_code    TEXT NOT NULL,
        close_price REAL DEFAULT 0,
        open_price  REAL DEFAULT 0,
        high_price  REAL DEFAULT 0,
        low_price   REAL DEFAULT 0,
        volume      REAL DEFAULT 0,
        chg_amt     REAL DEFAULT 0,
        chg_pct     REAL DEFAULT 0,
        nav         REAL DEFAULT 0,
        premium_pct REAL DEFAULT 0,
        aum_billion REAL DEFAULT 0,
        PRIMARY KEY (trade_date, etf_code)
    );

    CREATE TABLE IF NOT EXISTS fetch_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date   TEXT,
        etf_code   TEXT,
        status     TEXT,
        records    INTEGER DEFAULT 0,
        error_msg  TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    CREATE INDEX IF NOT EXISTS idx_holdings_date ON daily_holdings(trade_date);
    CREATE INDEX IF NOT EXISTS idx_changes_date  ON holdings_changes(trade_date);
    CREATE INDEX IF NOT EXISTS idx_etf_prices    ON etf_prices(trade_date, etf_code);
    """)
    conn.commit()
    conn.close()
    log.info("✓ 資料庫初始化完成")


# ══════════════════════════════════════════════════════════
# 2. 抓取持股（Pocket.tw M722）
# ══════════════════════════════════════════════════════════
def fetch_pocket_holdings(etf_code: str) -> list[dict]:
    param = (
        f"AssignID%3D{etf_code}%3B"
        "MTPeriod%3D0%3BDTMode%3D0%3BDTRange%3D1%3BDTOrder%3D1%3BMajorTable%3DM722%3B"
    )
    url = (
        "https://www.pocket.tw/api/cm/MobileService/ashx/GetDtnoData.ashx"
        f"?action=getdtnodata&DtNo=59449513&ParamStr={param}&FilterNo=0"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        raw = resp.json().get('Data', [])
        holdings = []
        for row in raw:
            if len(row) < 5:
                continue
            unit = row[5] if len(row) > 5 else ''
            if unit != '股':
                continue
            stock_code = str(row[1]).strip()
            if not stock_code.isdigit():
                continue
            try:
                holdings.append({
                    'stock_code': stock_code,
                    'stock_name': str(row[2]).strip(),
                    'weight_pct': float(row[3]),
                    'shares':     float(str(row[4]).replace(',', '')),
                })
            except (ValueError, TypeError):
                continue
        return holdings
    except Exception as e:
        log.error(f"  ✗ {etf_code} 持股抓取失敗: {e}")
        return []


# ══════════════════════════════════════════════════════════
# 3. 抓取個股收盤價（TWSE OpenAPI）
# ══════════════════════════════════════════════════════════
def fetch_stock_close_prices(stock_codes: set) -> dict[str, float]:
    try:
        resp = requests.get(
            'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code != 200:
            return {}
        prices = {}
        for item in resp.json():
            code = item.get('Code', '')
            if code in stock_codes:
                try:
                    prices[code] = float(item['ClosingPrice'].replace(',', ''))
                except (ValueError, KeyError):
                    pass
        return prices
    except Exception as e:
        log.error(f"個股收盤價抓取失敗: {e}")
        return {}


# ══════════════════════════════════════════════════════════
# 4. 抓取 ETF 今日收盤價（TWSE 上市 ETF 行情）
#    來源：TWSE ETF 每日行情 API
# ══════════════════════════════════════════════════════════
def fetch_etf_prices_today(trade_date: str) -> dict[str, dict]:
    """
    從 TWSE 抓取今日所有ETF收盤價
    回傳 { etf_code: { close, open, high, low, volume, chg_amt, chg_pct } }
    """
    results = {}
    try:
        # 上市 ETF 日行情
        resp = requests.get(
            'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code == 200:
            all_data = {item.get('Code', ''): item for item in resp.json()}
            for code in ALL_PRICE_ETFS:
                item = all_data.get(code)
                if not item:
                    continue
                try:
                    close = float(item.get('ClosingPrice', '0').replace(',', '') or 0)
                    open_ = float(item.get('OpeningPrice', '0').replace(',', '') or 0)
                    high  = float(item.get('HighestPrice', '0').replace(',', '') or 0)
                    low   = float(item.get('LowestPrice',  '0').replace(',', '') or 0)
                    vol   = float(item.get('TradeVolume',  '0').replace(',', '') or 0)
                    chg   = item.get('Change', '0').replace(',', '').replace('+', '') or '0'
                    chg_amt = float(chg) if chg not in ('', '--', '除息', 'X') else 0.0
                    prev  = close - chg_amt
                    chg_pct = round(chg_amt / prev * 100, 2) if prev > 0 else 0.0
                    if close > 0:
                        results[code] = {
                            'close': close, 'open': open_, 'high': high,
                            'low': low, 'volume': vol,
                            'chg_amt': chg_amt, 'chg_pct': chg_pct,
                        }
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        log.error(f"ETF今日收盤價抓取失敗: {e}")

    log.info(f"✓ 今日ETF收盤價：{len(results)} 檔")
    return results


# ══════════════════════════════════════════════════════════
# 5. 抓取 ETF NAV 淨值（TWSE 每日淨值公告）
# ══════════════════════════════════════════════════════════
def fetch_etf_nav(trade_date: str) -> dict[str, float]:
    """
    從 TWSE 抓取 ETF 每日基金淨值
    回傳 { etf_code: nav_price }
    """
    nav_map = {}
    try:
        # TWSE ETF淨值API（單位淨值）
        date_nodash = trade_date.replace('-', '')
        url = f"https://www.twse.com.tw/fund/TWT38U?response=json&date={date_nodash}&_={int(time.time()*1000)}"
        resp = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.twse.com.tw/'
        }, timeout=20)
        data = resp.json()
        if data.get('stat') == 'OK':
            for row in data.get('data', []):
                if len(row) < 4:
                    continue
                code = str(row[0]).strip()
                if code in ALL_PRICE_ETFS:
                    try:
                        nav_str = str(row[3]).replace(',', '').strip()
                        nav = float(nav_str)
                        if nav > 0:
                            nav_map[code] = nav
                    except (ValueError, IndexError):
                        continue
    except Exception as e:
        log.warning(f"ETF NAV抓取失敗（非致命）: {e}")

    log.info(f"✓ ETF NAV：{len(nav_map)} 檔")
    return nav_map


# ══════════════════════════════════════════════════════════
# 6. 抓取 ETF 規模（TWSE 基金規模）
# ══════════════════════════════════════════════════════════
def fetch_etf_aum() -> dict[str, float]:
    """
    從 TWSE 抓取 ETF 基金規模（億元）
    回傳 { etf_code: aum_billion }
    """
    aum_map = {}
    try:
        # TWSE ETF規模資訊
        url = "https://www.twse.com.tw/fund/TWT07U?response=json"
        resp = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.twse.com.tw/'
        }, timeout=20)
        data = resp.json()
        if data.get('stat') == 'OK':
            for row in data.get('data', []):
                if len(row) < 5:
                    continue
                code = str(row[0]).strip()
                if code in ALL_PRICE_ETFS:
                    try:
                        # 規模通常是第4或第5欄（千元），轉換成億
                        aum_str = str(row[4]).replace(',', '').strip()
                        aum_thousand = float(aum_str)
                        aum_billion  = round(aum_thousand / 100000, 2)  # 千元 → 億
                        if aum_billion > 0:
                            aum_map[code] = aum_billion
                    except (ValueError, IndexError):
                        continue
    except Exception as e:
        log.warning(f"ETF規模抓取失敗（非致命）: {e}")

    log.info(f"✓ ETF規模：{len(aum_map)} 檔")
    return aum_map


# ══════════════════════════════════════════════════════════
# 7. 抓取 ETF 歷史收盤價（補齊過去資料供績效圖使用）
#    第一次執行時抓過去 400 天，之後每日只補當天
# ══════════════════════════════════════════════════════════
def fetch_etf_price_history(etf_code: str, start_date: str, end_date: str) -> list[dict]:
    """
    從 TWSE 月份行情API 抓取 ETF 歷史收盤價
    按月份分批抓取，避免單次請求過大
    """
    from datetime import datetime
    records = []
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end   = datetime.strptime(end_date,   '%Y-%m-%d')

    current = start.replace(day=1)
    while current <= end:
        ym = current.strftime('%Y%m') + '01'
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={ym}&stockNo={etf_code}"
        try:
            resp = requests.get(url, headers={
                'User-Agent': 'Mozilla/5.0',
                'Referer': 'https://www.twse.com.tw/'
            }, timeout=20)
            data = resp.json()
            if data.get('stat') == 'OK':
                for row in data.get('data', []):
                    if len(row) < 7:
                        continue
                    try:
                        # 民國日期轉西元
                        parts = row[0].replace('/', '-').split('-')
                        year  = int(parts[0]) + 1911
                        td    = f"{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                        close = float(str(row[6]).replace(',', ''))
                        open_ = float(str(row[3]).replace(',', ''))
                        high  = float(str(row[4]).replace(',', ''))
                        low   = float(str(row[5]).replace(',', ''))
                        vol   = float(str(row[1]).replace(',', ''))
                        if close > 0:
                            records.append({
                                'trade_date': td,
                                'close': close, 'open': open_,
                                'high': high,   'low': low,
                                'volume': vol,
                            })
                    except (ValueError, IndexError):
                        continue
        except Exception as e:
            log.warning(f"  {etf_code} {ym[:6]} 歷史價格抓取失敗: {e}")

        # 移到下個月
        if current.month == 12:
            current = current.replace(year=current.year+1, month=1)
        else:
            current = current.replace(month=current.month+1)
        time.sleep(0.5)

    return records


def backfill_etf_prices():
    """
    補抓所有ETF的歷史收盤價（第一次執行時跑）
    之後每日只補當天，不重複抓歷史
    """
    conn = sqlite3.connect(DB_PATH)

    # 檢查已有多少歷史資料
    existing = pd.read_sql(
        "SELECT etf_code, COUNT(*) as cnt FROM etf_prices GROUP BY etf_code",
        conn
    )
    conn.close()

    existing_map = dict(zip(existing['etf_code'], existing['cnt'])) if not existing.empty else {}

    today     = date.today()
    # 往回抓400天（主動ETF最早約2025年初上市，有多少抓多少）
    start_400 = (today - timedelta(days=400)).strftime('%Y-%m-%d')
    end_str   = today.strftime('%Y-%m-%d')

    for code in ALL_PRICE_ETFS:
        existing_cnt = existing_map.get(code, 0)
        if existing_cnt > 200:
            # 已有大量歷史，跳過補抓
            log.info(f"  {code} 已有 {existing_cnt} 筆歷史，跳過補抓")
            continue

        log.info(f"  補抓 {code} 歷史收盤價（{start_400} ~ {end_str}）...")
        records = fetch_etf_price_history(code, start_400, end_str)
        if records:
            _save_etf_price_records(code, records)
            log.info(f"  ✓ {code} 補抓 {len(records)} 筆")
        else:
            log.warning(f"  ✗ {code} 無歷史資料")
        time.sleep(1)


def _save_etf_price_records(etf_code: str, records: list[dict]):
    """將ETF歷史價格存入DB"""
    conn = sqlite3.connect(DB_PATH)
    for r in records:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO etf_prices
                (trade_date, etf_code, close_price, open_price, high_price, low_price, volume)
                VALUES (?,?,?,?,?,?,?)
            """, (r['trade_date'], etf_code,
                  r['close'], r.get('open', 0), r.get('high', 0),
                  r.get('low', 0), r.get('volume', 0)))
        except sqlite3.Error:
            pass
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════
# 8. 存入 daily_holdings
# ══════════════════════════════════════════════════════════
def save_holdings(etf_code: str, trade_date: str,
                  holdings: list[dict], prices: dict[str, float]) -> int:
    conn = sqlite3.connect(DB_PATH)
    saved = 0
    for h in holdings:
        price  = prices.get(h['stock_code'], 0)
        amount = h['shares'] * price if price > 0 else 0
        try:
            conn.execute("""
                INSERT OR REPLACE INTO daily_holdings
                (trade_date, etf_code, stock_code, stock_name,
                 weight_pct, shares, close_price, amount_est)
                VALUES (?,?,?,?,?,?,?,?)
            """, (trade_date, etf_code,
                  h['stock_code'], h['stock_name'],
                  h['weight_pct'], h['shares'], price, amount))
            saved += 1
        except sqlite3.Error as e:
            log.error(f"存檔失敗 {etf_code}/{h['stock_code']}: {e}")
    conn.commit()
    conn.close()
    return saved


# ══════════════════════════════════════════════════════════
# 9. 存入 ETF 今日價格
# ══════════════════════════════════════════════════════════
def save_etf_prices_today(trade_date: str,
                          prices: dict[str, dict],
                          nav_map: dict[str, float],
                          aum_map: dict[str, float]):
    conn = sqlite3.connect(DB_PATH)
    for code, p in prices.items():
        nav         = nav_map.get(code, 0)
        aum         = aum_map.get(code, 0)
        premium_pct = round((p['close'] - nav) / nav * 100, 2) if nav > 0 else 0
        try:
            conn.execute("""
                INSERT OR REPLACE INTO etf_prices
                (trade_date, etf_code, close_price, open_price, high_price,
                 low_price, volume, chg_amt, chg_pct, nav, premium_pct, aum_billion)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (trade_date, code,
                  p['close'], p.get('open', 0), p.get('high', 0),
                  p.get('low', 0),  p.get('volume', 0),
                  p.get('chg_amt', 0), p.get('chg_pct', 0),
                  nav, premium_pct, aum))
        except sqlite3.Error as e:
            log.error(f"ETF價格存檔失敗 {code}: {e}")
    conn.commit()
    conn.close()
    log.info(f"✓ ETF今日價格儲存 {len(prices)} 檔")


# ══════════════════════════════════════════════════════════
# 10. 偵測今昨持股變化
# ══════════════════════════════════════════════════════════
def detect_changes(trade_date: str, yesterday: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df_t = pd.read_sql("SELECT * FROM daily_holdings WHERE trade_date=?", conn, params=[trade_date])
    df_y = pd.read_sql("SELECT * FROM daily_holdings WHERE trade_date=?", conn, params=[yesterday])

    if df_y.empty:
        log.info(f"⚠ 無昨日資料（{yesterday}），跳過變化偵測")
        conn.close()
        return pd.DataFrame()

    merged = pd.merge(
        df_t[['etf_code','stock_code','stock_name','weight_pct','shares','close_price']],
        df_y[['etf_code','stock_code','weight_pct','shares']],
        on=['etf_code','stock_code'], how='outer', suffixes=('_t','_y')
    ).fillna(0)

    changes = []
    for _, r in merged.iterrows():
        wt, wy = r['weight_pct_t'], r['weight_pct_y']
        diff   = round(wt - wy, 4)
        if abs(diff) < 0.05:
            continue
        if wy == 0:    action = 'NEW_BUY'
        elif wt == 0:  action = 'FULL_SELL'
        elif diff > 0: action = 'INCREASE'
        else:          action = 'DECREASE'
        price      = r.get('close_price', 0)
        shares_chg = r['shares_t'] - r['shares_y']
        changes.append({
            'trade_date':    trade_date,
            'etf_code':      r['etf_code'],
            'stock_code':    r['stock_code'],
            'stock_name':    r.get('stock_name', ''),
            'action':        action,
            'weight_before': round(wy, 4),
            'weight_after':  round(wt, 4),
            'weight_change': diff,
            'shares_change': round(shares_chg, 0),
            'close_price':   price,
            'amount_change': round(shares_chg * price, 0),
        })

    if changes:
        df_c = pd.DataFrame(changes)
        df_c.to_sql('holdings_changes', conn, if_exists='append', index=False, method='multi')
        conn.close()
        log.info(f"✓ 偵測到 {len(df_c)} 筆持股變化")
        return df_c

    conn.close()
    return pd.DataFrame()


# ══════════════════════════════════════════════════════════
# 11. 輸出排行榜（終端機）
# ══════════════════════════════════════════════════════════
def print_rankings(df_changes: pd.DataFrame, df_holdings: pd.DataFrame):
    print("\n" + "=" * 65)
    if not df_changes.empty:
        buy = (df_changes[df_changes['amount_change'] > 0]
               .groupby(['stock_code','stock_name'])
               .agg(ETF數=('etf_code','nunique'), 買入金額=('amount_change','sum'))
               .sort_values('買入金額', ascending=False).head(20))
        print("📈 共同買入 TOP20"); print(buy.to_string())

        sell = (df_changes[df_changes['amount_change'] < 0]
                .groupby(['stock_code','stock_name'])
                .agg(ETF數=('etf_code','nunique'), 賣出金額=('amount_change','sum'))
                .sort_values('賣出金額', ascending=True).head(20))
        print("\n📉 共同賣出 TOP20"); print(sell.to_string())
    else:
        print("🏆 共識持股排行（被最多ETF同時持有）")
        consensus = (df_holdings
                     .groupby(['stock_code','stock_name'])
                     .agg(ETF數=('etf_code','nunique'), 總權重=('weight_pct','sum'), 總市值估算=('amount_est','sum'))
                     .sort_values('ETF數', ascending=False).head(20))
        print(consensus.to_string())
    print("=" * 65 + "\n")


# ══════════════════════════════════════════════════════════
# 12. 匯出 JSON（供 dashboard.html 讀取）
# ══════════════════════════════════════════════════════════
def export_json(trade_date: str,
                df_holdings: pd.DataFrame,
                df_changes:  pd.DataFrame,
                etf_prices_today: dict,
                nav_map: dict,
                aum_map: dict):
    import datetime

    # ── summary.json ──────────────────────────────────────
    summary = {
        'date':           trade_date,
        'etf_count':      len(ACTIVE_ETFS),
        'total_holdings': len(df_holdings),
        'has_changes':    not df_changes.empty,
        'change_count':   len(df_changes),
        'updated_at':     datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source':         'Pocket.tw M722 API',
        'active_etfs':    [{'code': k, 'name': v} for k, v in ACTIVE_ETFS.items()],
    }
    _wj('data/summary.json', summary)

    # ── holdings.json（完整持股，不截斷）─────────────────
    holdings_dict = {}
    for etf_code, etf_name in ACTIVE_ETFS.items():
        sub = (df_holdings[df_holdings['etf_code'] == etf_code]
               .sort_values('weight_pct', ascending=False)
               [['stock_code','stock_name','weight_pct','shares','close_price','amount_est']]
               .to_dict('records'))
        holdings_dict[etf_code] = {'name': etf_name, 'holdings': sub}
    _wj('data/holdings.json', holdings_dict)

    # ── etf_prices.json（ETF價格、NAV、規模）────────────
    prices_out = {}
    for code, name in ALL_PRICE_ETFS.items():
        p = etf_prices_today.get(code, {})
        nav = nav_map.get(code, 0)
        aum = aum_map.get(code, 0)
        premium = round((p.get('close', 0) - nav) / nav * 100, 2) if nav > 0 else 0
        prices_out[code] = {
            'name':        name,
            'close':       p.get('close',   0),
            'open':        p.get('open',    0),
            'high':        p.get('high',    0),
            'low':         p.get('low',     0),
            'volume':      p.get('volume',  0),
            'chg_amt':     p.get('chg_amt', 0),
            'chg_pct':     p.get('chg_pct', 0),
            'nav':         nav,
            'premium_pct': premium,
            'aum_billion': aum,
            'is_benchmark': code in BENCHMARK_ETFS,
        }
    _wj('data/etf_prices.json', prices_out)

    # ── performance.json（近12個月每月報酬率，供績效圖）─
    _export_performance(trade_date)

    if not df_changes.empty:
        buy = (df_changes[df_changes['amount_change'] > 0]
               .groupby(['stock_code','stock_name'])
               .agg(etf_count=('etf_code','nunique'),
                    value=('amount_change','sum'),
                    etf_list=('etf_code', lambda x: list(x.unique())))
               .sort_values('value', ascending=False)
               .reset_index().head(20).to_dict('records'))
        _wj('data/buy_ranking.json', buy)

        sell = (df_changes[df_changes['amount_change'] < 0]
                .groupby(['stock_code','stock_name'])
                .agg(etf_count=('etf_code','nunique'),
                     value=('amount_change','sum'),
                     etf_list=('etf_code', lambda x: list(x.unique())))
                .sort_values('value', ascending=True)
                .reset_index().head(20).to_dict('records'))
        _wj('data/sell_ranking.json', sell)

        df_changes.to_json('data/daily_changes.json', orient='records', force_ascii=False)
    else:
        # 第一天：用共識持股填入買入排行
        consensus = (df_holdings
                     .groupby(['stock_code','stock_name'])
                     .agg(etf_count=('etf_code','nunique'), value=('amount_est','sum'))
                     .sort_values('etf_count', ascending=False)
                     .reset_index().head(20).to_dict('records'))
        _wj('data/buy_ranking.json', consensus)
        _wj('data/sell_ranking.json', [])
        _wj('data/daily_changes.json', [])

    log.info("✓ JSON 匯出完成 → data/")


def _export_performance(trade_date: str):
    """
    從 etf_prices 計算近12個月每月報酬率
    格式：{ etf_code: { '2025-01': 3.2, '2025-02': -1.1, ... } }
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("""
            SELECT etf_code, trade_date, close_price
            FROM etf_prices
            WHERE close_price > 0
            ORDER BY etf_code, trade_date
        """, conn)
    except Exception:
        conn.close()
        _wj('data/performance.json', {})
        return
    conn.close()

    if df.empty:
        _wj('data/performance.json', {})
        return

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df['ym'] = df['trade_date'].dt.to_period('M')

    # 每月取最後一個交易日的收盤價
    monthly = (df.groupby(['etf_code','ym'])
               .apply(lambda g: g.loc[g['trade_date'].idxmax(), 'close_price'])
               .reset_index(name='close'))
    monthly['ym_str'] = monthly['ym'].astype(str)
    monthly = monthly.sort_values(['etf_code','ym_str'])

    perf = {}
    for code, grp in monthly.groupby('etf_code'):
        grp = grp.reset_index(drop=True)
        monthly_returns = {}
        for i in range(1, len(grp)):
            ym  = grp.loc[i, 'ym_str']
            cur = grp.loc[i,   'close']
            prv = grp.loc[i-1, 'close']
            if prv > 0:
                monthly_returns[ym] = round((cur - prv) / prv * 100, 2)
        perf[code] = monthly_returns

    _wj('data/performance.json', perf)
    log.info(f"✓ 績效資料匯出：{len(perf)} 檔")


def _wj(path: str, obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def run(target_date: str | None = None):
    if target_date:
        td = date.fromisoformat(target_date)
    else:
        td = date.today()
    if td.weekday() == 5: td -= timedelta(days=1)
    if td.weekday() == 6: td -= timedelta(days=2)

    today_str = td.strftime('%Y-%m-%d')
    yd = td - timedelta(days=1)
    if yd.weekday() == 6: yd -= timedelta(days=2)
    elif yd.weekday() == 5: yd -= timedelta(days=1)
    yesterday_str = yd.strftime('%Y-%m-%d')

    log.info(f"{'='*60}")
    log.info(f"=== 開始執行 {today_str}（昨日：{yesterday_str}）===")

    init_db()

    # 第一次執行時補抓歷史價格（之後因有200筆以上會自動跳過）
    log.info("檢查並補抓 ETF 歷史收盤價...")
    backfill_etf_prices()

    # 批次抓取 14 檔持股
    all_holdings, all_codes = [], set()
    for etf_code, etf_name in ACTIVE_ETFS.items():
        h = fetch_pocket_holdings(etf_code)
        if h:
            for item in h:
                item['etf_code'] = etf_code
            all_holdings.extend(h)
            all_codes.update(item['stock_code'] for item in h)
            log.info(f"  ✓ {etf_code} {etf_name}: {len(h)} 筆")
        else:
            log.warning(f"  ✗ {etf_code} {etf_name}: 無資料")
        time.sleep(1.5)

    log.info(f"共抓取 {len(all_holdings)} 筆持股（{len(all_codes)} 支股票）")

    # 抓個股收盤價
    stock_prices = fetch_stock_close_prices(all_codes)
    log.info(f"✓ 取得 {len(stock_prices)} 支個股收盤價")

    # 存入 daily_holdings
    total_saved = 0
    for etf_code in ACTIVE_ETFS:
        etf_h = [h for h in all_holdings if h['etf_code'] == etf_code]
        if etf_h:
            total_saved += save_holdings(etf_code, today_str, etf_h, stock_prices)
    log.info(f"✓ 儲存 {total_saved} 筆持股")

    # 抓 ETF 今日收盤價、NAV、規模
    etf_prices_today = fetch_etf_prices_today(today_str)
    nav_map  = fetch_etf_nav(today_str)
    aum_map  = fetch_etf_aum()

    # 存入 etf_prices
    save_etf_prices_today(today_str, etf_prices_today, nav_map, aum_map)

    # 偵測持股變化
    conn = sqlite3.connect(DB_PATH)
    df_today = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?", conn, params=[today_str])
    conn.close()

    df_changes = detect_changes(today_str, yesterday_str)

    # 排行榜
    print_rankings(df_changes, df_today)

    # 匯出 JSON
    export_json(today_str, df_today, df_changes, etf_prices_today, nav_map, aum_map)

    log.info(f"✅ 完成！{today_str} 共 {len(df_today)} 筆持股")


if __name__ == '__main__':
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else None)
