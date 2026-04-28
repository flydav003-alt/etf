"""
fetch_holdings.py — 台股主動式ETF持股抓取系統
資料來源：Pocket.tw M722 API（持股）
         TWSE OpenAPI（個股收盤價、ETF收盤價）
         Pocket.tw 網頁（NAV淨值、折溢價、規模 — Playwright渲染）
"""

import requests
import sqlite3
import pandas as pd
import json
import time
import logging
import asyncio
import re
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

    # ── DB 遷移：補舊版 etf_prices 表缺少的欄位 ──────────
    # 第一次用新版時，舊表只有 close_price，需要補其他欄位
    migrations = [
        "ALTER TABLE etf_prices ADD COLUMN open_price  REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN high_price  REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN low_price   REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN volume      REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN chg_amt     REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN chg_pct     REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN nav         REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN premium_pct REAL DEFAULT 0",
        "ALTER TABLE etf_prices ADD COLUMN aum_billion REAL DEFAULT 0",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # 欄位已存在，忽略

    conn.close()
    log.info("✓ 資料庫初始化完成")


# ══════════════════════════════════════════════════════════
# 2. 抓取持股（Pocket.tw M722）
# ══════════════════════════════════════════════════════════
def fetch_pocket_holdings(etf_code: str, debug: bool = False) -> list[dict]:
    """
    抓取 ETF 持股清單。
    - debug=True：印出 raw response 的前 5 筆,以及所有被過濾掉的條目
                  （用來確認 M722 是否包含現金部位、現金欄位長什麼樣）
    - 回傳結構含股票持股 + 一筆特殊 stock_code='CASH' 的現金部位（若 API 有提供）
    """
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
        json_resp = resp.json()
        raw = json_resp.get('Data', [])

        # ── DEBUG：印出原始格式,協助確認現金欄位 ──────────
        if debug:
            log.info(f"  [DEBUG] {etf_code} 原始回傳 Title: {json_resp.get('Title')}")
            log.info(f"  [DEBUG] {etf_code} 原始回傳前 5 筆:")
            for i, row in enumerate(raw[:5]):
                log.info(f"    [{i}] {row}")
            units = {}
            for row in raw:
                if len(row) > 5:
                    u = row[5] if row[5] else '(空字串)'
                    units[u] = units.get(u, 0) + 1
            log.info(f"  [DEBUG] {etf_code} 單位種類統計: {units}")
            non_stock = [r for r in raw if len(r) > 5 and r[5] != '股']
            if non_stock:
                log.info(f"  [DEBUG] {etf_code} 非股條目 ({len(non_stock)} 筆):")
                for row in non_stock[:10]:
                    log.info(f"    {row}")

        holdings = []
        for row in raw:
            if len(row) < 5:
                continue
            unit = row[5] if len(row) > 5 else ''
            stock_code_raw = str(row[1]).strip() if len(row) > 1 else ''
            stock_name_raw = str(row[2]).strip() if len(row) > 2 else ''

            # ── 嘗試擷取現金部位 ─────────────────────────
            # M722 現金欄位實測：
            #   stock_code='C_NTD', stock_name='CASH', unit='元'
            #   stock_code='C_USD', stock_name='CASH', unit='元' 等
            # 判斷條件：unit 是「元」，或 stock_name=='CASH'，或 stock_code 非純數字
            cash_keywords = ('現金', '銀行', '存款', '活存', 'CASH', 'TWD', '新台幣')
            is_cash = (
                unit != '股' and (
                    stock_name_raw == 'CASH' or
                    unit == '元' or
                    any(kw in stock_name_raw for kw in cash_keywords) or
                    (stock_code_raw and not stock_code_raw.isdigit() and
                     stock_code_raw.startswith('C_'))
                )
            )
            if is_cash:
                try:
                    holdings.append({
                        'stock_code': 'CASH',
                        'stock_name': stock_name_raw or '現金部位',
                        'weight_pct': float(row[3]) if row[3] not in (None, '') else 0,
                        'shares':     0,
                    })
                    log.debug(f"  {etf_code} 偵測到現金部位: {stock_name_raw} = {row[3]}%")
                except (ValueError, TypeError):
                    pass
                continue

            # ── 一般股票持股 ──────────────────────────────
            if unit != '股':
                continue
            if not stock_code_raw.isdigit():
                continue
            try:
                holdings.append({
                    'stock_code': stock_code_raw,
                    'stock_name': stock_name_raw,
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
# 3. 抓取個股收盤價（TWSE 上市 + TPEx 上櫃）
# ══════════════════════════════════════════════════════════
def fetch_stock_close_prices(stock_codes: set) -> dict[str, float]:
    """
    同時從 TWSE（上市）和 TPEx（上櫃）抓取收盤價，合併回傳。
    - TWSE: openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
    - TPEx: openapi.tpex.org.tw/v1/exchangeReport/STOCK_DAY_ALL
    兩個都抓，TPEx 補齊 TWSE 抓不到的上櫃股票（約佔成分股 20%~30%）
    """
    prices = {}

    # ── TWSE 上市 ────────────────────────────────────────
    try:
        resp = requests.get(
            'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code == 200:
            for item in resp.json():
                code = item.get('Code', '')
                if code in stock_codes and code not in prices:
                    try:
                        prices[code] = float(item['ClosingPrice'].replace(',', ''))
                    except (ValueError, KeyError):
                        pass
            log.info(f"  TWSE 上市：取得 {len(prices)} 支收盤價")
        else:
            log.warning(f"  TWSE STOCK_DAY_ALL 回傳 {resp.status_code}")
    except Exception as e:
        log.error(f"  TWSE 收盤價抓取失敗: {e}")

    # ── TPEx 上櫃 ────────────────────────────────────────
    tpex_got = 0
    try:
        resp = requests.get(
            'https://openapi.tpex.org.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code == 200:
            for item in resp.json():
                code = item.get('Code', '')
                if code in stock_codes and code not in prices:
                    try:
                        prices[code] = float(item['ClosingPrice'].replace(',', ''))
                        tpex_got += 1
                    except (ValueError, KeyError):
                        pass
            log.info(f"  TPEx 上櫃：補充 {tpex_got} 支收盤價")
        else:
            log.warning(f"  TPEx STOCK_DAY_ALL 回傳 {resp.status_code}")
    except Exception as e:
        log.warning(f"  TPEx 收盤價抓取失敗（不影響主流程）: {e}")

    return prices


# ══════════════════════════════════════════════════════════
# 4. 抓取 ETF 今日收盤價
#    來源：TWSE STOCK_DAY_ALL（ETF 也在上市股票清單內）
#           TPEx STOCK_DAY_ALL 補充上櫃掛牌的 ETF
# ══════════════════════════════════════════════════════════
def fetch_etf_prices_today(trade_date: str) -> dict[str, dict]:
    """
    從 TWSE + TPEx STOCK_DAY_ALL 抓取 ETF 今日收盤價
    回傳 { etf_code: { close, open, high, low, volume, chg_amt, chg_pct } }
    """
    results = {}

    def _parse_source(all_data):
        for code in ALL_PRICE_ETFS:
            if code in results:
                continue
            item = all_data.get(code)
            if not item:
                continue
            try:
                def _f(key, fallback='0'):
                    v = item.get(key, fallback) or fallback
                    return float(str(v).replace(',', '').replace('+', '').strip() or '0')
                close   = _f('ClosingPrice')
                open_   = _f('OpeningPrice')
                high    = _f('HighestPrice')
                low     = _f('LowestPrice')
                vol     = _f('TradeVolume')
                chg_raw = str(item.get('Change', '0') or '0').replace(',', '').replace('+', '').strip()
                try:
                    chg_amt = float(chg_raw) if chg_raw and chg_raw not in ('--','X','除息','除權','除權息') else 0.0
                except ValueError:
                    chg_amt = 0.0
                prev    = close - chg_amt
                chg_pct = round(chg_amt / prev * 100, 2) if prev > 0 else 0.0
                if close > 0:
                    results[code] = {
                        'close': close, 'open': open_, 'high': high,
                        'low': low,     'volume': vol,
                        'chg_amt': chg_amt, 'chg_pct': chg_pct,
                    }
            except (ValueError, TypeError) as e:
                log.debug(f"ETF價格解析失敗 {code}: {e}")

    # TWSE 上市
    try:
        resp = requests.get(
            'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code == 200:
            _parse_source({item.get('Code', ''): item for item in resp.json()})
        else:
            log.warning(f"TWSE STOCK_DAY_ALL 回傳 {resp.status_code}")
    except Exception as e:
        log.error(f"TWSE ETF收盤價抓取失敗: {e}")

    # TPEx 上櫃補充
    try:
        resp = requests.get(
            'https://openapi.tpex.org.tw/v1/exchangeReport/STOCK_DAY_ALL',
            timeout=25
        )
        if resp.status_code == 200:
            before = len(results)
            _parse_source({item.get('Code', ''): item for item in resp.json()})
            if len(results) > before:
                log.info(f"  TPEx 補充 {len(results)-before} 檔 ETF 收盤價")
    except Exception as e:
        log.warning(f"TPEx ETF收盤價抓取失敗（不影響主流程）: {e}")

    log.info(f"✓ 今日ETF收盤價：{len(results)} 檔")
    return results


# ══════════════════════════════════════════════════════════
# 5+6. 同時抓取 ETF NAV 淨值 + 規模
#    完全照 Grok colab 驗證版本 + 加強 NAV 正規表達式
# ══════════════════════════════════════════════════════════
async def _fetch_one_etf_nav_aum(context, code: str) -> dict:
    """完全照 Grok get_etf_info，NAV正規表達式加強版"""
    from bs4 import BeautifulSoup

    result = {
        "etf_code":             code,
        "market_price":         None,
        "nav":                  None,
        "premium_discount_pct": None,
        "scale_billion":        None,
        "source":               None,
        "success":              False,
    }

    base_code = code.lower()
    urls = [
        ("MoneyDJ",         f"https://www.moneydj.com/etf/x/basic/basic0004.xdjhtm?etfid={base_code}.tw"),
        ("Pocket_Discount", f"https://www.pocket.tw/etf/tw/{code}/discountpremium/"),
        ("Pocket_Main",     f"https://www.pocket.tw/etf/tw/{code}"),
    ]

    for name, url in urls:
        page = None
        try:
            page = await context.new_page()
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(8000)

            content_html = await page.content()
            soup = BeautifulSoup(content_html, "html.parser")
            text = soup.get_text(separator=" ", strip=True)

            # ── 偵錯：印出含「淨值」的上下文 ──
            nav_idx = text.find("淨值")
            if nav_idx >= 0:
                log.debug(f"  [{name}] {code} 淨值上下文: ...{text[max(0,nav_idx-5):nav_idx+30]}...")

            # 市價（照 Grok）
            if not result["market_price"]:
                m = re.search(r'(\d{2}\.\d{1,3})\s*[▲▼]', text)
                if m:
                    result["market_price"] = float(m.group(1))

            # 規模（照 Grok）
            if not result["scale_billion"]:
                s = re.search(r'規模.*?([\d,\.]+)\s*億', text) or \
                    re.search(r'資產規模.*?([\d,\.]+)', text)
                if s:
                    result["scale_billion"] = float(s.group(1).replace(",", ""))

            # 淨值 — 多種正規表達式，放寬位數限制
            if not result["nav"]:
                nav_patterns = [
                    r'淨值\s*[:：]?\s*([\d]+\.[\d]{1,4})',
                    r'NAV\s*[:：]?\s*([\d]+\.[\d]{1,4})',
                    r'每單位淨資產\s*[:：]?\s*([\d]+\.[\d]{1,4})',
                    r'昨日淨值\s*[:：]?\s*([\d]+\.[\d]{1,4})',
                    r'基金淨值\s*[:：]?\s*([\d]+\.[\d]{1,4})',
                ]
                for pat in nav_patterns:
                    n = re.search(pat, text)
                    if n:
                        val = float(n.group(1))
                        if 1 < val < 10000:
                            result["nav"] = val
                            break

            # 折溢價（照 Grok）
            if not result["premium_discount_pct"]:
                pd_m = re.search(r'折溢價.*?([-\d\.]+)%', text)
                if pd_m:
                    pd_val = float(pd_m.group(1))
                    if abs(pd_val) < 10:
                        result["premium_discount_pct"] = pd_val

            # 照 Grok：有市價+規模就成功
            if result["market_price"] and result["scale_billion"]:
                result["success"] = True
                result["source"]  = name
                await page.close()
                break

        except Exception as e:
            log.debug(f"  [{name}] {code} 失敗: {e}")
        finally:
            if page and not page.is_closed():
                try: await page.close()
                except: pass

    return result


def fetch_etf_nav_and_aum() -> tuple[dict, dict]:
    nav_map  = {}
    aum_map  = {}
    prem_map = {}

    try:
        from playwright.async_api import async_playwright
        from bs4 import BeautifulSoup  # noqa
    except ImportError as e:
        log.warning(f"缺少套件，跳過 NAV/規模抓取：{e}")
        return nav_map, aum_map

    async def _run():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            for code in ALL_PRICE_ETFS:
                r = await _fetch_one_etf_nav_aum(ctx, code)
                if r["nav"] is not None:
                    nav_map[code] = r["nav"]
                if r["scale_billion"] is not None:
                    aum_map[code] = r["scale_billion"]
                if r["premium_discount_pct"] is not None:
                    prem_map[code] = r["premium_discount_pct"]
                log.info(
                    f"  {code}: NAV={r['nav']}, AUM={r['scale_billion']}億, "
                    f"折溢價={r['premium_discount_pct']}%, 來源={r['source']}"
                )
                await asyncio.sleep(1)
            await browser.close()

    try:
        asyncio.run(_run())
    except RuntimeError:
        try:
            import nest_asyncio
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(_run())
        except Exception as e:
            log.error(f"Playwright 執行失敗: {e}")

    log.info(f"✓ ETF NAV：{len(nav_map)} 檔，規模：{len(aum_map)} 檔，折溢價：{len(prem_map)} 檔")
    nav_map["_prem_map"] = prem_map
    return nav_map, aum_map


# 相容舊呼叫名稱
def fetch_etf_nav(trade_date: str = '') -> dict[str, float]:
    nav_map, _ = fetch_etf_nav_and_aum()
    return nav_map

def fetch_etf_aum() -> dict[str, float]:
    _, aum_map = fetch_etf_nav_and_aum()
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
                          aum_map: dict[str, float],
                          prem_map: dict[str, float] | None = None):
    if prem_map is None:
        prem_map = {}
    conn = sqlite3.connect(DB_PATH)
    for code, p in prices.items():
        nav = nav_map.get(code, 0) or 0
        aum = aum_map.get(code, 0) or 0
        # 優先用 Playwright 抓到的折溢價，沒有才用計算值
        if code in prem_map and prem_map[code] is not None:
            premium_pct = prem_map[code]
        elif nav > 0:
            premium_pct = round((p['close'] - nav) / nav * 100, 2)
        else:
            premium_pct = 0
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

    df_t = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?",
        conn, params=[trade_date]
    )

    # ★ 修正：不靠傳入的 yesterday，直接查 DB 裡最近一個有資料的交易日
    # 這樣週一也能正確找到上週五，不受週末計算邏輯影響
    prev_dates = pd.read_sql("""
        SELECT DISTINCT trade_date FROM daily_holdings
        WHERE trade_date < ?
        ORDER BY trade_date DESC
        LIMIT 1
    """, conn, params=[trade_date])

    if prev_dates.empty:
        log.info(f"⚠ DB 中無前一交易日資料，跳過變化偵測（首次執行正常）")
        conn.close()
        return pd.DataFrame()

    prev_date = prev_dates.iloc[0]['trade_date']
    log.info(f"  比對日期：今日={trade_date}，前一交易日={prev_date}")

    df_y = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?",
        conn, params=[prev_date]
    )

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
        # 先刪今天舊紀錄,避免重跑時 UNIQUE constraint 衝突
        conn.execute(
            "DELETE FROM holdings_changes WHERE trade_date = ?", (trade_date,)
        )
        conn.commit()
        df_c.to_sql('holdings_changes', conn, if_exists='append', index=False, method='multi')
        conn.close()
        log.info(f"✓ 偵測到 {len(df_c)} 筆持股變化")
        return df_c

    conn.close()
    return pd.DataFrame()


# ══════════════════════════════════════════════════════════
# 10b. 計算連續加碼/減碼天數（Streak）
# ══════════════════════════════════════════════════════════
def compute_streaks(today_str: str, lookback_days: int = 10) -> tuple[dict, dict]:
    """
    從 holdings_changes 表往回查 lookback_days 個交易日,
    計算每個 (etf, stock) 的連續加碼/減碼天數。

    規則：
    - 「加碼」= action 為 NEW_BUY 或 INCREASE
    - 「減碼」= action 為 FULL_SELL 或 DECREASE
    - 「連續」從今天往回看,每個交易日都同方向才算
    - 中間有跳天（沒出現）或反向動作 → streak 中斷
    - 只回傳 streak >= 2 的條目

    回傳：
    - by_etf_stock: { etf_code: { stock_code: {buy: int, sell: int} } }
    - by_stock:     { stock_code: {max_buy, max_sell, etf_count_buy, etf_count_sell} }
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT trade_date, etf_code, stock_code, action
        FROM holdings_changes
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
    """, conn, params=[today_str])
    conn.close()

    if df.empty:
        log.info("⚠ holdings_changes 為空,無 streak 可計算")
        return {}, {}

    all_dates = sorted(df['trade_date'].unique(), reverse=True)[:lookback_days]
    BUY_ACT  = {'NEW_BUY', 'INCREASE'}
    SELL_ACT = {'FULL_SELL', 'DECREASE'}

    by_etf_stock: dict = {}
    for (etf, stock), grp in df.groupby(['etf_code', 'stock_code']):
        action_by_date = dict(zip(grp['trade_date'], grp['action']))
        buy_streak  = 0
        sell_streak = 0
        for d in all_dates:
            act = action_by_date.get(d)
            if act in BUY_ACT:
                if sell_streak > 0:
                    break
                buy_streak += 1
            elif act in SELL_ACT:
                if buy_streak > 0:
                    break
                sell_streak += 1
            else:
                break
        if buy_streak >= 2 or sell_streak >= 2:
            by_etf_stock.setdefault(etf, {})[stock] = {
                'buy':  buy_streak  if buy_streak  >= 2 else 0,
                'sell': sell_streak if sell_streak >= 2 else 0,
            }

    by_stock: dict = {}
    for etf, stocks in by_etf_stock.items():
        for stock, sk in stocks.items():
            entry = by_stock.setdefault(stock, {
                'max_buy': 0, 'max_sell': 0,
                'etf_count_buy': 0, 'etf_count_sell': 0,
            })
            if sk['buy'] >= 2:
                entry['max_buy']        = max(entry['max_buy'], sk['buy'])
                entry['etf_count_buy'] += 1
            if sk['sell'] >= 2:
                entry['max_sell']        = max(entry['max_sell'], sk['sell'])
                entry['etf_count_sell'] += 1

    n_etf_stock = sum(len(s) for s in by_etf_stock.values())
    log.info(f"✓ Streak 計算完成：{n_etf_stock} 筆 ETF×股票連續紀錄,"
             f"{len(by_stock)} 支股票有彙總 streak")
    return by_etf_stock, by_stock


def export_streaks_json(by_etf_stock: dict, by_stock: dict, today_str: str):
    """匯出 streak 結果到 data/streaks.json"""
    out = {
        'date': today_str,
        'by_etf_stock': by_etf_stock,
        'by_stock':     by_stock,
    }
    _wj('data/streaks.json', out)
    log.info(f"✓ streaks.json 已匯出")


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

    # ── etf_prices.json（ETF價格、NAV、規模、折溢價）──────
    # 從 DB 讀取今日已存的 etf_prices（包含 Playwright 抓到的 NAV/折溢價）
    conn_ep = sqlite3.connect(DB_PATH)
    df_ep = pd.read_sql(
        "SELECT * FROM etf_prices WHERE trade_date=?",
        conn_ep, params=[trade_date]
    )
    conn_ep.close()

    prices_out = {}
    for code, name in ALL_PRICE_ETFS.items():
        p     = etf_prices_today.get(code, {})
        ep_row = df_ep[df_ep['etf_code']==code].iloc[0].to_dict() if not df_ep[df_ep['etf_code']==code].empty else {}
        nav          = ep_row.get('nav', 0) or 0
        premium_pct  = ep_row.get('premium_pct', 0) or 0
        aum          = ep_row.get('aum_billion', 0) or 0
        prices_out[code] = {
            'name':         name,
            'close':        p.get('close',   0),
            'open':         p.get('open',    0),
            'high':         p.get('high',    0),
            'low':          p.get('low',     0),
            'volume':       p.get('volume',  0),
            'chg_amt':      p.get('chg_amt', 0),
            'chg_pct':      p.get('chg_pct', 0),
            'nav':          nav,
            'premium_pct':  premium_pct,
            'aum_billion':  aum,
            'is_benchmark': code in BENCHMARK_ETFS,
        }
    _wj('data/etf_prices.json', prices_out)

    # ── performance.json（近12個月每月報酬率，供績效圖）─
    _export_performance(trade_date)

    # ── price_history.json（每日收盤價，供走勢圖使用）────
    _export_price_history()

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


def _export_price_history():
    """
    匯出每檔 ETF 的每日收盤價歷史
    供前端走勢圖計算任意時間區間的累積報酬率
    格式：{ etf_code: [ {date:'2025-01-02', close:18.5}, ... ] }
    只保留最近 400 天，控制 JSON 大小
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("""
            SELECT etf_code, trade_date, close_price
            FROM etf_prices
            WHERE close_price > 0
            ORDER BY etf_code, trade_date DESC
        """, conn)
    except Exception as e:
        log.warning(f"price_history 匯出失敗: {e}")
        conn.close()
        _wj('data/price_history.json', {})
        return
    conn.close()

    if df.empty:
        _wj('data/price_history.json', {})
        return

    history = {}
    for code, grp in df.groupby('etf_code'):
        # 最近 400 天，倒序變正序
        rows = grp.head(400).sort_values('trade_date')
        history[code] = [
            {'date': row['trade_date'], 'close': round(row['close_price'], 2)}
            for _, row in rows.iterrows()
        ]

    _wj('data/price_history.json', history)
    log.info(f"✓ 價格歷史匯出：{len(history)} 檔")


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
    # 第一檔（00981A）開 debug,印出原始 raw response 與被過濾條目
    # 想看其他檔請在 Actions 設環境變數 DEBUG_HOLDINGS_ETF=00982A
    import os
    debug_target = os.environ.get('DEBUG_HOLDINGS_ETF', '00981A')
    all_holdings, all_codes = [], set()
    for etf_code, etf_name in ACTIVE_ETFS.items():
        is_debug = (etf_code == debug_target)
        h = fetch_pocket_holdings(etf_code, debug=is_debug)
        if h:
            for item in h:
                item['etf_code'] = etf_code
            all_holdings.extend(h)
            all_codes.update(item['stock_code'] for item in h if item['stock_code'] != 'CASH')
            cash_count = sum(1 for item in h if item['stock_code'] == 'CASH')
            cash_note  = f"(含現金 {cash_count})" if cash_count else ""
            log.info(f"  ✓ {etf_code} {etf_name}: {len(h)} 筆 {cash_note}")
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

    # 抓 ETF 今日收盤價、NAV、規模（NAV和AUM合併一次呼叫）
    etf_prices_today = fetch_etf_prices_today(today_str)
    nav_map, aum_map = fetch_etf_nav_and_aum()

    # 取出折溢價 map（夾帶在 nav_map["_prem_map"] 裡）
    prem_map = nav_map.pop("_prem_map", {})

    # 存入 etf_prices（傳入 prem_map 讓折溢價也寫進 DB）
    save_etf_prices_today(today_str, etf_prices_today, nav_map, aum_map, prem_map)

    # 偵測持股變化
    conn = sqlite3.connect(DB_PATH)
    df_today = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?", conn, params=[today_str])
    conn.close()

    df_changes = detect_changes(today_str, yesterday_str)

    # 計算連續加碼/減碼天數（streak）
    by_etf_stock, by_stock = compute_streaks(today_str, lookback_days=10)
    export_streaks_json(by_etf_stock, by_stock, today_str)

    # 排行榜
    print_rankings(df_changes, df_today)

    # 匯出 JSON
    export_json(today_str, df_today, df_changes, etf_prices_today, nav_map, aum_map)

    log.info(f"✅ 完成！{today_str} 共 {len(df_today)} 筆持股")


if __name__ == '__main__':
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else None)
