"""
fetch_holdings.py — 台股主動式ETF持股抓取系統
資料來源：Pocket.tw M722 API（已驗證可直接呼叫）
收盤價：TWSE OpenAPI
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
# 設定 logging（同時輸出到終端機和 etf_tracker.log）
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
# ★ 只需改這裡就能新增 / 移除追蹤的 ETF ★
#
# 新增主動 ETF：在 ACTIVE_ETFS 加一行
#   '代號': '中文名稱',
#
# 新增績效基準：在 BENCHMARK_ETFS 加一行
#   （只抓價格，不做持股比對）
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

BENCHMARK_ETFS = {
    '0050':   '元大台灣50',
    '009816': '凱基台灣TOP50',
}

# ══════════════════════════════════════════════════════════
# 常數設定
# ══════════════════════════════════════════════════════════
DB_PATH   = 'etf_tracker.db'
DATA_DIR  = Path('data')
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
    'Referer':    'https://www.pocket.tw/etf/tw/00981A/fundholding',
}


# ══════════════════════════════════════════════════════════
# 1. 資料庫初始化
# ══════════════════════════════════════════════════════════
def init_db():
    """建立 SQLite 資料庫與所有資料表（若已存在則略過）"""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
    -- 每日持股明細（主力表）
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

    -- 每日持股變化（今昨比對結果）
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

    -- ETF 每日價格（基準用）
    CREATE TABLE IF NOT EXISTS etf_prices (
        trade_date  TEXT NOT NULL,
        etf_code    TEXT NOT NULL,
        close_price REAL DEFAULT 0,
        chg_pct     REAL DEFAULT 0,
        PRIMARY KEY (trade_date, etf_code)
    );

    -- 執行日誌（方便除錯）
    CREATE TABLE IF NOT EXISTS fetch_log (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date   TEXT,
        etf_code   TEXT,
        status     TEXT,
        records    INTEGER DEFAULT 0,
        error_msg  TEXT,
        created_at TEXT DEFAULT (datetime('now','localtime'))
    );

    -- 建立索引加速查詢
    CREATE INDEX IF NOT EXISTS idx_holdings_date ON daily_holdings(trade_date);
    CREATE INDEX IF NOT EXISTS idx_changes_date  ON holdings_changes(trade_date);
    """)
    conn.commit()
    conn.close()
    log.info("✓ 資料庫初始化完成")


# ══════════════════════════════════════════════════════════
# 2. 抓取持股（Pocket.tw M722 API）
# ══════════════════════════════════════════════════════════
def fetch_pocket_holdings(etf_code: str) -> list[dict]:
    """
    從 Pocket.tw M722 API 抓取 ETF 持股明細
    回傳欄位：stock_code / stock_name / weight_pct / shares
    只保留 unit=='股' 的台股持股（排除現金、債券）
    """
    param = (
        f"AssignID%3D{etf_code}%3B"
        "MTPeriod%3D0%3B"
        "DTMode%3D0%3B"
        "DTRange%3D1%3B"
        "DTOrder%3D1%3B"
        "MajorTable%3DM722%3B"
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
            # 確認欄位數足夠
            if len(row) < 5:
                continue
            unit = row[5] if len(row) > 5 else ''
            # 只保留股票（排除現金、受益憑證等）
            if unit != '股':
                continue
            stock_code = str(row[1]).strip()
            # 只保留純數字代碼（台股上市股票）
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
        log.error(f"  ✗ {etf_code} 抓取失敗: {e}")
        return []


# ══════════════════════════════════════════════════════════
# 3. 抓取個股收盤價（TWSE OpenAPI）
# ══════════════════════════════════════════════════════════
def fetch_close_prices(stock_codes: set) -> dict[str, float]:
    """
    從 TWSE OpenAPI 抓取所有上市股票收盤價
    非交易日會回傳空陣列，此時回傳空 dict（不影響其他欄位）
    """
    try:
        resp = requests.get(
            'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code != 200:
            log.warning(f"TWSE OpenAPI 回傳 {resp.status_code}，可能非交易日")
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
        log.error(f"收盤價抓取失敗: {e}")
        return {}


# ══════════════════════════════════════════════════════════
# 4. 抓取基準 ETF 收盤價（0050、009816）
# ══════════════════════════════════════════════════════════
def fetch_benchmark_prices(trade_date: str) -> dict[str, dict]:
    """
    從 TWSE 抓取基準 ETF 的收盤價與漲跌
    回傳格式：{ '0050': {'close': 178.5, 'chg_pct': 0.45} }
    """
    results = {}
    try:
        resp = requests.get(
            'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code != 200:
            return results

        for item in resp.json():
            code = item.get('Code', '')
            if code in BENCHMARK_ETFS:
                try:
                    close = float(item['ClosingPrice'].replace(',', ''))
                    chg   = float(item.get('Change', '0').replace(',', '').replace('+', ''))
                    chg_pct = round(chg / (close - chg) * 100, 2) if (close - chg) != 0 else 0
                    results[code] = {'close': close, 'chg_pct': chg_pct}
                except (ValueError, KeyError, ZeroDivisionError):
                    pass
    except Exception as e:
        log.error(f"基準ETF價格抓取失敗: {e}")

    return results


# ══════════════════════════════════════════════════════════
# 5. 存入 daily_holdings
# ══════════════════════════════════════════════════════════
def save_holdings(etf_code: str, trade_date: str,
                  holdings: list[dict], prices: dict[str, float]) -> int:
    """將持股存入 daily_holdings，計算 amount_est = shares × close_price"""
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
# 6. 偵測今昨持股變化
# ══════════════════════════════════════════════════════════
def detect_changes(trade_date: str, yesterday: str) -> pd.DataFrame:
    """
    比對今日與昨日 daily_holdings，產生 holdings_changes
    action 分類：
      NEW_BUY   = 昨天沒有、今天有
      FULL_SELL = 昨天有、今天沒有
      INCREASE  = 持股比重增加
      DECREASE  = 持股比重減少
    忽略 weight 差異 < 0.05% 的細微變動（可能是估值誤差）
    """
    conn = sqlite3.connect(DB_PATH)

    df_t = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?",
        conn, params=[trade_date]
    )
    df_y = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?",
        conn, params=[yesterday]
    )

    if df_y.empty:
        log.info(f"⚠ 無昨日資料（{yesterday}），跳過變化偵測，明日起自動比對")
        conn.close()
        return pd.DataFrame()

    # 合併今昨資料，比對每個 ETF + 股票 的組合
    merged = pd.merge(
        df_t[['etf_code', 'stock_code', 'stock_name',
              'weight_pct', 'shares', 'close_price']],
        df_y[['etf_code', 'stock_code', 'weight_pct', 'shares']],
        on=['etf_code', 'stock_code'],
        how='outer',
        suffixes=('_t', '_y')
    ).fillna(0)

    changes = []
    for _, r in merged.iterrows():
        wt   = r['weight_pct_t']
        wy   = r['weight_pct_y']
        diff = round(wt - wy, 4)

        # 忽略微小變動
        if abs(diff) < 0.05:
            continue

        # 判斷動作類型
        if wy == 0:
            action = 'NEW_BUY'
        elif wt == 0:
            action = 'FULL_SELL'
        elif diff > 0:
            action = 'INCREASE'
        else:
            action = 'DECREASE'

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
        df_c.to_sql('holdings_changes', conn,
                    if_exists='append', index=False,
                    method='multi')
        conn.close()
        log.info(f"✓ 偵測到 {len(df_c)} 筆持股變化")
        return df_c

    conn.close()
    log.info("今日無持股變化（或差異均在誤差範圍內）")
    return pd.DataFrame()


# ══════════════════════════════════════════════════════════
# 7. 排行榜輸出（終端機印出，方便 GitHub Actions log 檢視）
# ══════════════════════════════════════════════════════════
def print_rankings(df_changes: pd.DataFrame, df_holdings: pd.DataFrame):
    print("\n" + "=" * 65)

    if not df_changes.empty:
        # ── 買入排行 ──────────────────────────────────────────
        buy = (df_changes[df_changes['amount_change'] > 0]
               .groupby(['stock_code', 'stock_name'])
               .agg(ETF數=('etf_code', 'nunique'),
                    買入金額=('amount_change', 'sum'))
               .sort_values('買入金額', ascending=False)
               .head(20))
        print("📈 共同買入 TOP20（依估算金額）")
        print(buy.to_string())

        # ── 賣出排行 ──────────────────────────────────────────
        sell = (df_changes[df_changes['amount_change'] < 0]
                .groupby(['stock_code', 'stock_name'])
                .agg(ETF數=('etf_code', 'nunique'),
                     賣出金額=('amount_change', 'sum'))
                .sort_values('賣出金額', ascending=True)
                .head(20))
        print("\n📉 共同賣出 TOP20（依估算金額）")
        print(sell.to_string())

    else:
        # 第一天無昨日資料時，顯示共識持股排行
        print("🏆 共識持股排行（被最多 ETF 同時持有）")
        consensus = (df_holdings
                     .groupby(['stock_code', 'stock_name'])
                     .agg(ETF數=('etf_code', 'nunique'),
                          總權重=('weight_pct', 'sum'),
                          總市值估算=('amount_est', 'sum'))
                     .sort_values('ETF數', ascending=False)
                     .head(20))
        print(consensus.to_string())

    print("=" * 65 + "\n")


# ══════════════════════════════════════════════════════════
# 8. 匯出 JSON（供 dashboard.html 讀取）
# ══════════════════════════════════════════════════════════
def export_json(trade_date: str,
                df_holdings: pd.DataFrame,
                df_changes: pd.DataFrame,
                benchmark_prices: dict):
    """
    產生 data/ 資料夾下所有 JSON 檔，dashboard.html 直接讀取這些檔案
    ├── summary.json         總覽 + 最後更新時間
    ├── holdings.json        各 ETF 完整持股（key = etf_code）
    ├── buy_ranking.json     共同買入排行
    ├── sell_ranking.json    共同賣出排行
    ├── daily_changes.json   今日所有異動明細
    └── benchmark.json       0050 / 009816 價格
    """
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
        'active_etfs':    [
            {'code': k, 'name': v} for k, v in ACTIVE_ETFS.items()
        ],
    }
    _write_json('data/summary.json', summary)

    # ── holdings.json ─────────────────────────────────────
    # 每個 ETF 的前 20 大持股（依 weight_pct 降序）
    holdings_dict = {}
    for etf_code, etf_name in ACTIVE_ETFS.items():
        sub = (df_holdings[df_holdings['etf_code'] == etf_code]
               .sort_values('weight_pct', ascending=False)
               .head(20)
               [['stock_code', 'stock_name', 'weight_pct',
                 'shares', 'close_price', 'amount_est']]
               .to_dict('records'))
        holdings_dict[etf_code] = {
            'name':     etf_name,
            'holdings': sub,
        }
    _write_json('data/holdings.json', holdings_dict)

    if not df_changes.empty:
        # ── daily_changes.json ────────────────────────────
        df_changes.to_json(
            'data/daily_changes.json',
            orient='records', force_ascii=False
        )

        # ── buy_ranking.json ──────────────────────────────
        buy = (df_changes[df_changes['amount_change'] > 0]
               .groupby(['stock_code', 'stock_name'])
               .agg(etf_count=('etf_code', 'nunique'),
                    value=('amount_change', 'sum'),
                    etf_list=('etf_code', lambda x: list(x.unique())))
               .sort_values('value', ascending=False)
               .reset_index()
               .head(20)
               .to_dict('records'))
        _write_json('data/buy_ranking.json', buy)

        # ── sell_ranking.json ─────────────────────────────
        sell = (df_changes[df_changes['amount_change'] < 0]
                .groupby(['stock_code', 'stock_name'])
                .agg(etf_count=('etf_code', 'nunique'),
                     value=('amount_change', 'sum'),
                     etf_list=('etf_code', lambda x: list(x.unique())))
                .sort_values('value', ascending=True)
                .reset_index()
                .head(20)
                .to_dict('records'))
        _write_json('data/sell_ranking.json', sell)

    else:
        # 第一天無變化時，用共識持股填入 buy_ranking（前端相容）
        consensus = (df_holdings
                     .groupby(['stock_code', 'stock_name'])
                     .agg(etf_count=('etf_code', 'nunique'),
                          value=('amount_est', 'sum'))
                     .sort_values('etf_count', ascending=False)
                     .reset_index()
                     .head(20)
                     .to_dict('records'))
        _write_json('data/buy_ranking.json', consensus)
        _write_json('data/sell_ranking.json', [])
        _write_json('data/daily_changes.json', [])

    # ── benchmark.json ────────────────────────────────────
    bench_out = {}
    for code, name in BENCHMARK_ETFS.items():
        p = benchmark_prices.get(code, {})
        bench_out[code] = {
            'name':  name,
            'close': p.get('close', 0),
            'chg_pct': p.get('chg_pct', 0),
        }
    _write_json('data/benchmark.json', bench_out)

    log.info("✓ JSON 匯出完成 → data/")


def _write_json(path: str, obj):
    """便利函數：寫入 JSON（UTF-8，縮排2）"""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


# ══════════════════════════════════════════════════════════
# MAIN — 主流程
# ══════════════════════════════════════════════════════════
def run(target_date: str | None = None):
    """
    完整執行一次抓取流程
    target_date: 'YYYY-MM-DD'，預設為今日（若是週末自動退到週五）
    """
    # 計算目標日期
    if target_date:
        td = date.fromisoformat(target_date)
    else:
        td = date.today()

    # 若是週末，退到最近的週五
    if td.weekday() == 5:   # 週六
        td -= timedelta(days=1)
    elif td.weekday() == 6: # 週日
        td -= timedelta(days=2)

    today_str = td.strftime('%Y-%m-%d')

    # 昨日（跳過週末）
    yd = td - timedelta(days=1)
    if yd.weekday() == 6:
        yd -= timedelta(days=2)
    elif yd.weekday() == 5:
        yd -= timedelta(days=1)
    yesterday_str = yd.strftime('%Y-%m-%d')

    log.info(f"{'='*60}")
    log.info(f"=== 開始執行 {today_str}（昨日：{yesterday_str}）===")
    log.info(f"{'='*60}")

    init_db()

    # ── 步驟1：批次抓取 14 檔持股 ────────────────────────
    all_holdings = []
    all_codes    = set()

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

        # 每檔之間暫停 1.5 秒，避免被封鎖
        time.sleep(1.5)

    log.info(f"共抓取 {len(all_holdings)} 筆持股（{len(all_codes)} 支股票）")

    # ── 步驟2：抓收盤價 ──────────────────────────────────
    log.info("抓取收盤價...")
    prices = fetch_close_prices(all_codes)
    log.info(f"✓ 取得 {len(prices)} 支收盤價")

    # ── 步驟3：存入資料庫 ────────────────────────────────
    total_saved = 0
    for etf_code in ACTIVE_ETFS:
        etf_holdings = [h for h in all_holdings if h['etf_code'] == etf_code]
        if etf_holdings:
            saved = save_holdings(etf_code, today_str, etf_holdings, prices)
            total_saved += saved
    log.info(f"✓ 儲存 {total_saved} 筆到資料庫")

    # ── 步驟4：偵測持股變化 ──────────────────────────────
    log.info("偵測持股變化...")
    df_changes = detect_changes(today_str, yesterday_str)

    # ── 步驟5：讀回今日持股 DataFrame ────────────────────
    conn = sqlite3.connect(DB_PATH)
    df_today = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?",
        conn, params=[today_str]
    )
    conn.close()

    # ── 步驟6：抓基準 ETF 價格 ───────────────────────────
    benchmark_prices = fetch_benchmark_prices(today_str)
    log.info(f"✓ 基準ETF: {list(benchmark_prices.keys())}")

    # ── 步驟7：排行榜輸出 ────────────────────────────────
    print_rankings(df_changes, df_today)

    # ── 步驟8：匯出 JSON ─────────────────────────────────
    export_json(today_str, df_today, df_changes, benchmark_prices)

    # ── 寫入執行日誌 ─────────────────────────────────────
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO fetch_log (run_date, etf_code, status, records)
        VALUES (?, 'ALL', 'OK', ?)
    """, (today_str, total_saved))
    conn.commit()
    conn.close()

    log.info(f"✅ 完成！{today_str} 共 {len(df_today)} 筆持股存入資料庫")


# ══════════════════════════════════════════════════════════
# 直接執行時的入口
# ══════════════════════════════════════════════════════════
if __name__ == '__main__':
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else None
    run(target)
