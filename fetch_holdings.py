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
import logging.handlers
import asyncio
import re
from datetime import date, timedelta
from pathlib import Path
from FinMind.data import DataLoader

# ══════════════════════════════════════════════════════════
# logging
# ══════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            'etf_tracker.log',
            maxBytes=5 * 1024 * 1024,  # 5MB 上限
            backupCount=3,              # 最多保留 3 個備份（共 20MB）
            encoding='utf-8'
        ),
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

    # ── TPEx 上櫃（多個備用 URL，DNS 解析失敗時自動換）────
    tpex_urls = [
        'https://openapi.tpex.org.tw/v1/exchangeReport/STOCK_DAY_ALL',
        'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes',
    ]
    tpex_got = 0
    for tpex_url in tpex_urls:
        try:
            resp = requests.get(tpex_url, timeout=25)
            if resp.status_code == 200:
                for item in resp.json():
                    code = item.get('Code', '') or item.get('SecuritiesCompanyCode', '')
                    if code in stock_codes and code not in prices:
                        try:
                            cp = item.get('ClosingPrice', '') or item.get('Close', '')
                            prices[code] = float(str(cp).replace(',', ''))
                            tpex_got += 1
                        except (ValueError, KeyError):
                            pass
                log.info(f"  TPEx 上櫃：補充 {tpex_got} 支收盤價（來源：{tpex_url.split('/')[2]}）")
                break  # 成功就不試備用
            else:
                log.warning(f"  TPEx {tpex_url.split('/')[2]} 回傳 {resp.status_code}，試備用...")
        except Exception as e:
            log.warning(f"  TPEx {tpex_url.split('/')[2]} 失敗（{e.__class__.__name__}），試備用...")

    return prices


# ══════════════════════════════════════════════════════════
# 4. 抓取 ETF 今日收盤價
#    來源：TWSE STOCK_DAY_ALL（ETF 也在上市股票清單內）
#           TPEx STOCK_DAY_ALL 補充上櫃掛牌的 ETF
# ══════════════════════════════════════════════════════════
def fetch_etf_prices_today(trade_date: str) -> dict[str, dict]:
    """
    使用 FinMind 抓取 ETF 收盤價 + 漲跌
    """
    results = {}
    dl = DataLoader()

    from datetime import datetime, timedelta
    end_date = trade_date
    start_date = (datetime.strptime(trade_date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')

    log.info(f"🔍 FinMind 抓取 ETF 價格（含漲跌）：{start_date} ~ {end_date}")

    for code in ALL_PRICE_ETFS:
        try:
            df = dl.taiwan_stock_daily(
                stock_id=code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                log.warning(f"  {code} 無資料")
                continue

            latest = df.iloc[-1].to_dict()

            close = float(latest.get('close', 0))
            if close <= 0:
                continue

            open_ = float(latest.get('open', 0))
            high = float(latest.get('max', 0))
            low = float(latest.get('min', 0))
            volume = float(latest.get('Trading_Volume', 0))

            # === 重點修正：多種方式取得漲跌 ===
            chg_amt = 0.0
            if 'change' in latest and latest['change'] not in (None, '', 0):
                chg_amt = float(latest['change'])
            elif 'yesterday_close' in latest and latest['yesterday_close'] not in (None, 0):
                yesterday_close = float(latest['yesterday_close'])
                chg_amt = close - yesterday_close
            else:
                # 最後手段：從前一天資料計算
                if len(df) >= 2:
                    prev_close = float(df.iloc[-2]['close'])
                    chg_amt = close - prev_close

            chg_pct = round(chg_amt / (close - chg_amt) * 100, 2) if (close - chg_amt) > 0 else 0.0

            results[code] = {
                'close': close,
                'open': open_,
                'high': high,
                'low': low,
                'volume': volume,
                'chg_amt': chg_amt,
                'chg_pct': chg_pct,
            }
            
            log.info(f"  ✓ {code} | 收盤 {close:.2f} | 漲跌 {chg_amt:+.2f} ({chg_pct:+.2f}%)")
            
        except Exception as e:
            log.error(f"FinMind 抓取 {code} 失敗: {e}")

    log.info(f"✓ FinMind 成功抓到 {len(results)} 檔 ETF（含漲跌）")
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
# 8b. 補抓 close_price=0 的持股（TWSE/TPEx 有時部分股票回傳空值）
# ══════════════════════════════════════════════════════════
def reprice_zero_holdings(trade_date: str) -> dict[str, float]:
    """
    對 daily_holdings 中 close_price=0 的持股，重新抓一次收盤價並更新 DB。
    回傳補到的 {stock_code: price}，讓呼叫端可以合併進記憶體的 price dict，
    確保 detect_changes 計算 amount_change 時用到完整的價格。

    【修法3】補抓率 < 70% 時，等 2 秒再重試一次（救 TPEx 偶發 DNS/速率失敗）
    """
    conn = sqlite3.connect(DB_PATH)
    missing = pd.read_sql("""
        SELECT DISTINCT stock_code FROM daily_holdings
        WHERE trade_date = ? AND close_price = 0 AND stock_code != 'CASH'
    """, conn, params=[trade_date])
    conn.close()

    if missing.empty:
        log.info("✓ 所有持股均有收盤價，無需補抓")
        return {}

    miss_codes = set(missing['stock_code'].tolist())
    log.info(f"  補抓 {len(miss_codes)} 支缺收盤價的股票：{sorted(miss_codes)}")

    prices = fetch_stock_close_prices(miss_codes)

    # ── 【修法3】補抓率太低時自動重試一次 ──
    # TPEx 上櫃股的兩個備用 URL 偶發 DNS 或速率失敗，
    # 第一次抓不到不代表股票真的查不到，等 2 秒重試通常會補上
    valid_count = sum(1 for p in prices.values() if p > 0)
    if miss_codes and valid_count < len(miss_codes) * 0.7:
        still_missing = miss_codes - {c for c, p in prices.items() if p > 0}
        log.warning(f"  補抓率偏低（{valid_count}/{len(miss_codes)}），2 秒後重試 {len(still_missing)} 支...")
        time.sleep(2)
        retry_prices = fetch_stock_close_prices(still_missing)
        retry_valid = sum(1 for p in retry_prices.values() if p > 0)
        prices.update({c: p for c, p in retry_prices.items() if p > 0})
        log.info(f"  ✓ 重試補上 {retry_valid} 支")

    if not prices:
        log.warning("  補抓收盤價：無結果，跳過")
        return {}

    conn = sqlite3.connect(DB_PATH)
    for code, price in prices.items():
        if price <= 0:
            continue
        conn.execute("""
            UPDATE daily_holdings
            SET close_price = ?,
                amount_est  = shares * ?
            WHERE trade_date = ? AND stock_code = ? AND close_price = 0
        """, (price, price, trade_date, code))
    conn.commit()
    conn.close()

    valid_final = sum(1 for p in prices.values() if p > 0)
    still_missing = len(miss_codes) - valid_final
    log.info(f"✓ 補價完成：更新 {valid_final} 支，仍缺 {still_missing} 支"
             f"（可能是未上市/下市/非台股）")
    return {c: p for c, p in prices.items() if p > 0}


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
        
        # === 重點修改：優先用現價與 NAV 計算折溢價 ===
        market_price = p.get('close', 0)
        
        if market_price > 0 and nav > 0:
            # 直接計算（最準確）
            premium_pct = round((market_price - nav) / nav * 100, 2)
            calc_source = "計算"
        elif code in prem_map and prem_map[code] is not None:
            # 備用：使用爬蟲抓到的折溢價
            premium_pct = prem_map[code]
            calc_source = "爬蟲"
        else:
            premium_pct = 0.0
            calc_source = "無資料"

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
            
            log.info(f"  {code} | 現價 {market_price} | NAV {nav} | "
                    f"折溢價 {premium_pct:+.2f}% ({calc_source})")
            
        except sqlite3.Error as e:
            log.error(f"ETF價格存檔失敗 {code}: {e}")
    
    conn.commit()
    conn.close()
    log.info(f"✓ ETF今日價格儲存 {len(prices)} 檔")


# ══════════════════════════════════════════════════════════
# 9b. 【修法5a】一次性回補:修舊的 stock_name="0" 記錄
#     從 daily_holdings 找該股票的歷史名稱回填到 holdings_changes
# ══════════════════════════════════════════════════════════
def backfill_full_sell_names():
    """
    舊版本 detect_changes 對 FULL_SELL 沒處理 stock_name,fillna(0) 後
    變成數字 0 寫進 DB。這個函數一次性把 holdings_changes 表裡這些
    stock_name 是 '0'/空字串/NULL 的記錄,從 daily_holdings 查回正確名字。
    每次跑都會檢查,但只更新 stock_name 真的有問題的記錄,負擔很小。
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        bad = pd.read_sql("""
            SELECT DISTINCT etf_code, stock_code, trade_date FROM holdings_changes
            WHERE stock_name = '0' OR stock_name = '' OR stock_name IS NULL
        """, conn)
    except Exception as e:
        log.warning(f"  回補 stock_name 失敗: {e}")
        conn.close()
        return

    if bad.empty:
        conn.close()
        return

    log.info(f"  發現 {len(bad)} 筆 holdings_changes 記錄 stock_name 不正確,開始回補...")
    fixed = 0
    for _, r in bad.iterrows():
        # 從 daily_holdings 找這支股票最近一次有正確名稱的記錄
        try:
            lookup = pd.read_sql("""
                SELECT stock_name FROM daily_holdings
                WHERE etf_code = ? AND stock_code = ?
                  AND stock_name != '' AND stock_name != '0' AND stock_name IS NOT NULL
                ORDER BY trade_date DESC LIMIT 1
            """, conn, params=[r['etf_code'], r['stock_code']])
            if not lookup.empty:
                sn = lookup.iloc[0]['stock_name']
                conn.execute("""
                    UPDATE holdings_changes SET stock_name = ?
                    WHERE trade_date = ? AND etf_code = ? AND stock_code = ?
                """, (sn, r['trade_date'], r['etf_code'], r['stock_code']))
                fixed += 1
        except Exception:
            continue
    conn.commit()
    conn.close()
    if fixed > 0:
        log.info(f"  ✓ 回補完成:更新 {fixed} 筆 stock_name")


# ══════════════════════════════════════════════════════════
# 9c. 【修法5b】找出最近一個「有意義」的交易日
#     用來決定 dashboard 要顯示哪天的資料
# ══════════════════════════════════════════════════════════
def find_display_date(today_str: str) -> tuple[str, bool]:
    """
    找出最近一個「有意義」的交易日,定義:同時有買入(NEW_BUY/INCREASE)
    和賣出(FULL_SELL/DECREASE)記錄的日期。

    用途:解決早晨跑腳本時 Pocket 還沒公告當日資料,但腳本還是抓到舊版本
    導致只偵測到零星 FULL_SELL、buy_ranking 一片空白的問題。

    規則:
    - 今日 holdings_changes 同時有買有賣 → display_date = today,is_fallback=False
    - 今日只有單向(只有買或只有賣)或沒資料 → 往回找最近一個雙向都有的日期
    - 都找不到 → 直接回 today_str(讓現有邏輯處理)

    回傳: (display_date, is_fallback)
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        # 【實際交易過濾】只看 shares_change != 0 的記錄
        # 否則早晨那種「全是權重漂移」的日子會被誤判成「有買有賣」不回退
        df = pd.read_sql("""
            SELECT trade_date, action FROM holdings_changes
            WHERE trade_date <= ?
              AND shares_change IS NOT NULL AND shares_change != 0
            ORDER BY trade_date DESC
        """, conn, params=[today_str])
    except Exception:
        conn.close()
        return today_str, False
    conn.close()

    if df.empty:
        return today_str, False

    BUY_ACT  = {'NEW_BUY', 'INCREASE'}
    SELL_ACT = {'FULL_SELL', 'DECREASE'}

    # 依日期分組,從最新開始檢查
    for date in df['trade_date'].drop_duplicates().tolist():
        actions = set(df[df['trade_date'] == date]['action'].tolist())
        has_buys  = bool(actions & BUY_ACT)
        has_sells = bool(actions & SELL_ACT)
        if has_buys and has_sells:
            return date, (date != today_str)

    # 找不到「有買有賣」的日期 → 用最新的(可能是首日只有買沒有賣)
    return df['trade_date'].iloc[0], (df['trade_date'].iloc[0] != today_str)


# ══════════════════════════════════════════════════════════
# 10. 偵測今昨持股變化
# ══════════════════════════════════════════════════════════
def detect_changes(trade_date: str, yesterday: str,
                   prices: dict[str, float] | None = None) -> pd.DataFrame:
    """
    比對今日與前一交易日持股變化。
    prices: 傳入 fetch_stock_close_prices 的結果（含補抓後的完整價格），
            直接用記憶體價格計算 amount_change，不依賴 DB 裡可能是 0 的 close_price。
    """
    conn = sqlite3.connect(DB_PATH)

    df_t = pd.read_sql(
        "SELECT * FROM daily_holdings WHERE trade_date=?",
        conn, params=[trade_date]
    )

    # 不靠傳入的 yesterday，直接查 DB 裡最近一個有資料的交易日
    # 這樣週一也能正確找到上週五
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
    conn.close()

    # ── 【修法2/4】把昨日的 close_price 跟 stock_name 都帶進來 ──
    # close_price → 今日抓不到時的備援
    # stock_name  → 修 FULL_SELL 時 stock_name="0" 的舊 bug:
    #   FULL_SELL 的股票今天不在 df_t,fillna(0) 會把 stock_name 變成數字 0
    #   要從 df_y 也帶 stock_name 進來,找不到今日就用昨日的
    merged = pd.merge(
        df_t[['etf_code','stock_code','stock_name','weight_pct','shares','close_price']],
        df_y[['etf_code','stock_code','stock_name','weight_pct','shares','close_price']],
        on=['etf_code','stock_code'], how='outer', suffixes=('_t','_y')
    )
    # stock_name 用空字串填(數值欄位才填 0,避免 stock_name 變成 0)
    merged['stock_name_t'] = merged['stock_name_t'].fillna('').astype(str)
    merged['stock_name_y'] = merged['stock_name_y'].fillna('').astype(str)
    for col in ['weight_pct_t','weight_pct_y','shares_t','shares_y',
                'close_price_t','close_price_y']:
        merged[col] = merged[col].fillna(0)

    changes = []
    zero_price_count = 0
    yday_fallback_count = 0
    name_fallback_count = 0
    for _, r in merged.iterrows():
        wt, wy = r['weight_pct_t'], r['weight_pct_y']
        diff   = round(wt - wy, 4)
        if abs(diff) < 0.05:
            continue
        if wy == 0:    action = 'NEW_BUY'
        elif wt == 0:  action = 'FULL_SELL'
        elif diff > 0: action = 'INCREASE'
        else:          action = 'DECREASE'

        # ── 【修法4】stock_name 雙層 fallback ──
        # 今日有 → 用今日;沒有(FULL_SELL) → 用昨日
        sn_t = (r['stock_name_t'] or '').strip()
        sn_y = (r['stock_name_y'] or '').strip()
        # "0" 是舊 fillna 殘留的字串,排除掉
        if sn_t and sn_t != '0':
            stock_name = sn_t
        elif sn_y and sn_y != '0':
            stock_name = sn_y
            name_fallback_count += 1
        else:
            stock_name = ''

        # ── 【修法2】三層 fallback 取得 close_price ──
        # 1. 記憶體裡的即時價格(已合併今日+前日 stock_codes 的查價結果)
        # 2. 今日 daily_holdings 的 close_price(可能是 reprice_zero_holdings 補抓到的)
        # 3. 昨日 daily_holdings 的 close_price(救 FULL_SELL 漏網之魚:API 暫時掛、新上市股等)
        price_today_db = r.get('close_price_t', 0) or 0
        price_yday_db  = r.get('close_price_y', 0) or 0
        price_mem      = (prices.get(r['stock_code'], 0) if prices else 0) or 0
        price = price_mem or price_today_db or price_yday_db
        if price == 0:
            zero_price_count += 1
        elif price_mem == 0 and price_today_db == 0 and price_yday_db > 0:
            yday_fallback_count += 1

        shares_chg = r['shares_t'] - r['shares_y']
        changes.append({
            'trade_date':    trade_date,
            'etf_code':      r['etf_code'],
            'stock_code':    r['stock_code'],
            'stock_name':    stock_name,
            'action':        action,
            'weight_before': round(wy, 4),
            'weight_after':  round(wt, 4),
            'weight_change': diff,
            'shares_change': round(shares_chg, 0),
            'close_price':   price,
            'amount_change': round(shares_chg * price, 0),
        })

    if not changes:
        return pd.DataFrame()

    df_c = pd.DataFrame(changes)

    if yday_fallback_count > 0:
        log.info(f"  ✓ 使用昨日收盤價救援 {yday_fallback_count} 筆異動金額")
    if name_fallback_count > 0:
        log.info(f"  ✓ 使用昨日股票名稱救援 {name_fallback_count} 筆 FULL_SELL 記錄")
    if zero_price_count > 0:
        log.warning(f"  ⚠ 仍有 {zero_price_count} 筆異動找不到任何收盤價（金額將為 0）")

    # ── 【核心過濾】只保留有實際股數變動的記錄，排除純權重漂移 ──
    # 權重漂移原因：ETF 沒有實際買賣，但其他成分股漲跌導致相對權重變化
    # 例如台積電大漲 → 台積電以外所有成分股「權重下降」，但基金根本沒賣任何股票
    # 只有 shares_change != 0 才是真正的買賣行為
    before_n = len(df_c)
    df_c = df_c[df_c['shares_change'] != 0].copy()
    drift_n = before_n - len(df_c)
    if drift_n > 0:
        log.info(f"  🔍 過濾純權重漂移：{before_n} 筆中移除 {drift_n} 筆（股數未變動），"
                 f"剩 {len(df_c)} 筆真實買賣")

    if df_c.empty:
        log.info("  ℹ 過濾後無真實交易記錄（今日持股股數與前日完全相同）")
        return pd.DataFrame()

    # 先刪今天舊紀錄，避免重跑時 UNIQUE constraint 衝突
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM holdings_changes WHERE trade_date = ?", (trade_date,))
    conn.commit()
    df_c.to_sql('holdings_changes', conn, if_exists='append', index=False, method='multi')
    conn.close()
    log.info(f"✓ 偵測到 {len(df_c)} 筆真實持股變化（已排除權重漂移）")
    return df_c


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
    # 【實際交易過濾】只算 shares_change != 0 的,排除純權重漂移
    # 否則股價波動會讓 streak 失真(連續加碼/減碼應該是真有買賣才算)
    df = pd.read_sql("""
        SELECT trade_date, etf_code, stock_code, action
        FROM holdings_changes
        WHERE trade_date <= ?
          AND shares_change IS NOT NULL AND shares_change != 0
        ORDER BY trade_date DESC
    """, conn, params=[today_str])
    conn.close()

    if df.empty:
        log.info("⚠ holdings_changes 為空(或無實際交易),無 streak 可計算")
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

    # ── 【修法5】決定 dashboard 要顯示哪天的資料 ──
    # 早晨腳本若還沒抓到當日完整資料(只偵測到零星 FULL_SELL),
    # display_date 會自動回退到最近一個「同時有買有賣」的交易日,
    # 讓前端不會出現空白。傍晚 cron 跑完當日完整資料後會自動切回今日。
    display_date, is_fallback = find_display_date(trade_date)
    if is_fallback:
        log.info(f"  📅 今日({trade_date})資料不完整,顯示日回退到 {display_date}")
        # 從 DB 抓 display_date 的 holdings_changes 來產生排行榜
        conn_d = sqlite3.connect(DB_PATH)
        df_display = pd.read_sql(
            "SELECT * FROM holdings_changes WHERE trade_date = ?",
            conn_d, params=[display_date]
        )
        conn_d.close()
    else:
        df_display = df_changes
        log.info(f"  📅 顯示日 = {display_date}(今日)")

    # ── summary.json ──────────────────────────────────────
    summary = {
        'date':           trade_date,           # 腳本實際跑的日期
        'display_date':   display_date,         # 前端顯示的資料日期
        'is_fallback':    is_fallback,          # True = 顯示的不是今日
        'etf_count':      len(ACTIVE_ETFS),
        'total_holdings': len(df_holdings),
        'has_changes':    not df_display.empty,
        'change_count':   len(df_display),
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

    # ── history.json（近 N 個交易日異動明細，供 modal 顯示走勢）──
    _export_history_changes(trade_date, days=10)

    # ── buy_ranking / sell_ranking / daily_changes 用 display_date 的資料 ──
    # 【實際交易過濾】只保留 shares_change != 0 的記錄,排除純權重漂移
    # （ETF 沒實際買賣、單純因其他股票價格波動導致權重變化的記錄）
    if not df_display.empty:
        before_n = len(df_display)
        df_real = df_display[df_display['shares_change'].fillna(0) != 0].copy()
        drift_n = before_n - len(df_real)
        if drift_n > 0:
            log.info(f"  📊 過濾權重漂移：{before_n} 筆中 {drift_n} 筆無實際交易，"
                     f"剩 {len(df_real)} 筆實際買賣")
    else:
        df_real = df_display

    if not df_real.empty:
        buy = (df_real[df_real['amount_change'] > 0]
               .groupby(['stock_code','stock_name'])
               .agg(etf_count=('etf_code','nunique'),
                    value=('amount_change','sum'),
                    etf_list=('etf_code', lambda x: list(x.unique())))
               .sort_values('value', ascending=False)
               .reset_index().head(20).to_dict('records'))
        _wj('data/buy_ranking.json', buy)

        sell = (df_real[df_real['amount_change'] < 0]
                .groupby(['stock_code','stock_name'])
                .agg(etf_count=('etf_code','nunique'),
                     value=('amount_change','sum'),
                     etf_list=('etf_code', lambda x: list(x.unique())))
                .sort_values('value', ascending=True)
                .reset_index().head(20).to_dict('records'))
        _wj('data/sell_ranking.json', sell)

        df_real.to_json('data/daily_changes.json', orient='records', force_ascii=False)
    else:
        # 第一天、DB 完全沒資料、或顯示日全是權重漂移：用共識持股填入買入排行
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


def _export_history_changes(today_str: str, days: int = 10):
    """
    匯出近 N 個交易日的所有持股異動,供前端 modal 顯示「過去 5 天動向」。

    回傳結構（兩種索引方式,前端任選一種使用）：
    {
      "date": "2026-04-29",
      "trade_dates": ["2026-04-29", "2026-04-28", ...],   # 涵蓋的交易日(由新到舊)
      "by_etf_stock": {                                    # 主索引:依 ETF + 股票分組
        "00981A": {
          "2330": [
            {"trade_date": "2026-04-29", "action": "INCREASE", ...},
            {"trade_date": "2026-04-28", "action": "INCREASE", ...},
            ...
          ]
        }
      },
      "by_stock": {                                        # 副索引:單支股票橫跨 ETF 的所有異動
        "2330": [{"trade_date":"2026-04-29","etf_code":"00981A",...}, ...]
      }
    }
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        # 【實際交易過濾】只抓 shares_change != 0 的記錄,排除純權重漂移
        # 先抓最近 N 個「有實際交易」的交易日
        dates_df = pd.read_sql("""
            SELECT DISTINCT trade_date FROM holdings_changes
            WHERE trade_date <= ?
              AND shares_change IS NOT NULL AND shares_change != 0
            ORDER BY trade_date DESC LIMIT ?
        """, conn, params=[today_str, days])

        if dates_df.empty:
            conn.close()
            _wj('data/history.json', {
                'date': today_str,
                'trade_dates': [],
                'by_etf_stock': {},
                'by_stock': {},
            })
            log.info("✓ 異動歷史匯出：無實際交易紀錄")
            return

        date_list = dates_df['trade_date'].tolist()
        placeholders = ','.join(['?'] * len(date_list))
        df = pd.read_sql(f"""
            SELECT trade_date, etf_code, stock_code, stock_name, action,
                   weight_before, weight_after, weight_change,
                   shares_change, close_price, amount_change
            FROM holdings_changes
            WHERE trade_date IN ({placeholders})
              AND shares_change IS NOT NULL AND shares_change != 0
            ORDER BY trade_date DESC, etf_code, stock_code
        """, conn, params=date_list)
    except Exception as e:
        log.warning(f"history.json 匯出失敗: {e}")
        conn.close()
        _wj('data/history.json', {})
        return
    conn.close()

    # 建主索引：by_etf_stock[etf_code][stock_code] = [異動列表]
    by_etf_stock: dict = {}
    by_stock:     dict = {}
    for _, r in df.iterrows():
        rec = {
            'trade_date':    r['trade_date'],
            'etf_code':      r['etf_code'],
            'stock_code':    r['stock_code'],
            'stock_name':    r['stock_name'],
            'action':        r['action'],
            'weight_before': float(r['weight_before']) if pd.notna(r['weight_before']) else 0,
            'weight_after':  float(r['weight_after'])  if pd.notna(r['weight_after'])  else 0,
            'weight_change': float(r['weight_change']) if pd.notna(r['weight_change']) else 0,
            'shares_change': float(r['shares_change']) if pd.notna(r['shares_change']) else 0,
            'close_price':   float(r['close_price'])   if pd.notna(r['close_price'])   else 0,
            'amount_change': float(r['amount_change']) if pd.notna(r['amount_change']) else 0,
        }
        by_etf_stock.setdefault(r['etf_code'], {}).setdefault(r['stock_code'], []).append(rec)
        by_stock.setdefault(r['stock_code'], []).append(rec)

    out = {
        'date':         today_str,
        'trade_dates':  date_list,
        'by_etf_stock': by_etf_stock,
        'by_stock':     by_stock,
        'total_records': len(df),
    }
    _wj('data/history.json', out)
    log.info(f"✓ 異動歷史匯出：{len(df)} 筆，涵蓋 {len(date_list)} 個交易日")


def _wj(path: str, obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


# ══════════════════════════════════════════════════════════
# 13. 匯出 CSV（共同加碼排行前10，供 BT repo 資料庫使用）
# ══════════════════════════════════════════════════════════
def export_etf_csv(trade_date: str, df_changes: pd.DataFrame) -> str | None:
    """
    從當日持股異動中，取出共同加碼排行前 10 檔，匯出 CSV。

    規格：
      - 欄位：stock_id, name, composite_score
      - composite_score = 各 ETF「當日合計買入金額」（holdings_changes.amount_change 加總）
        → 每天不同的實際買賣金額，不是累積持股的共識排行
      - 只計入實際有股數變動的買入記錄（排除純權重漂移）
      - 依 composite_score 降序排列，取前 10
      - 檔名：data/etf_YYYYMMDD.csv（日期取 display_date，與前端一致）

    與 export_json 相同邏輯：
      - 今日資料完整 → 用今日 df_changes
      - 今日資料不完整（剛跑完/公告還沒更新）→ 從 DB 讀最近完整交易日
      - 完全沒有歷史資料（系統首日）→ 回傳 None，不輸出
    """
    # ── 與 export_json 相同：找出有效的 display_date ──────
    # 避免今日資料只有零星 FULL_SELL、買入排行空白的問題
    display_date, is_fallback = find_display_date(trade_date)

    if is_fallback:
        # 今日資料不完整，從 DB 讀最近完整交易日的 holdings_changes
        log.info(f"  CSV 匯出：今日資料不完整，改用 {display_date} 的資料")
        conn = sqlite3.connect(DB_PATH)
        df_src = pd.read_sql(
            "SELECT * FROM holdings_changes WHERE trade_date = ?",
            conn, params=[display_date]
        )
        conn.close()
    else:
        df_src = df_changes

    if df_src.empty:
        # 系統首日，DB 裡完全沒有異動資料，正常現象
        log.info("CSV 匯出：無任何異動資料（系統首日），跳過")
        return None

    # 只取「實際買入」：金額 > 0 且有實際股數變動（排除純權重漂移）
    df_buy = df_src[
        (df_src['amount_change'] > 0) &
        (df_src['shares_change'].fillna(0) != 0)
    ].copy()

    if df_buy.empty:
        log.info(f"CSV 匯出：{display_date} 無實際買入記錄，跳過")
        return None

    # 彙總：依股票分組，加總各 ETF 的買入金額（今日實際成交估算）
    ranked = (
        df_buy
        .groupby(['stock_code', 'stock_name'])
        .agg(raw_score=('amount_change', 'sum'))
        .sort_values('raw_score', ascending=False)
        .head(10)
        .reset_index()
    )

    # composite_score 轉成「純數字」字串格式
    ranked['composite_score'] = (
        (ranked['raw_score'] / 1e8).round(1)
    )

    # 整理輸出欄位
    out = ranked[['stock_code', 'stock_name', 'composite_score']].copy()
    out.columns = ['stock_id', 'name', 'composite_score']

    # 檔名用 display_date（與前端顯示日一致），去掉連字號
    filename = f"data/etf_{display_date.replace('-', '')}.csv"
    out.to_csv(filename, index=False, encoding='utf-8-sig')  # utf-8-sig 讓 Excel 開啟不亂碼

    fallback_note = f"（回退自 {trade_date}）" if is_fallback else ""
    log.info(f"✓ CSV 匯出完成 → {filename}（{display_date}{fallback_note}，前 {len(out)} 檔）")
    log.info(f"  排行：{', '.join(out['name'].tolist())}")
    return filename


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

    # ── 【修法5a】一次性回補 holdings_changes 裡 stock_name="0" 的舊記錄 ──
    # 偵測到才更新,沒有就秒過,負擔很小
    backfill_full_sell_names()

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

    # ── 【修法1】把前一交易日的股票代碼也加入查價清單 ──
    # 解決 FULL_SELL（完全賣出）股票算不到金額的問題：
    # 被完全賣出的股票今天已不在持股裡，all_codes 不會包含它，
    # 結果它的收盤價沒抓到，detect_changes 計算 amount_change 時 price=0 → 金額顯示為 0
    today_only_count = len(all_codes)
    try:
        conn_prev = sqlite3.connect(DB_PATH)
        prev_dates = pd.read_sql(
            "SELECT DISTINCT trade_date FROM daily_holdings "
            "WHERE trade_date < ? ORDER BY trade_date DESC LIMIT 1",
            conn_prev, params=[today_str]
        )
        if not prev_dates.empty:
            prev_date = prev_dates.iloc[0]['trade_date']
            prev_stocks = pd.read_sql(
                "SELECT DISTINCT stock_code FROM daily_holdings "
                "WHERE trade_date=? AND stock_code != 'CASH'",
                conn_prev, params=[prev_date]
            )
            prev_codes = set(prev_stocks['stock_code'].tolist())
            extra_codes = prev_codes - all_codes
            all_codes.update(prev_codes)
            log.info(f"  合併前一交易日({prev_date})持股，新增 {len(extra_codes)} 支可能完全賣出的股票")
        conn_prev.close()
    except Exception as e:
        log.warning(f"  合併前一交易日持股失敗（可能首次執行）: {e}")

    # 抓個股收盤價
    stock_prices = fetch_stock_close_prices(all_codes)
    log.info(f"✓ 取得 {len(stock_prices)} 支個股收盤價（今日{today_only_count}支 + 前日新增）")

    # 存入 daily_holdings
    total_saved = 0
    for etf_code in ACTIVE_ETFS:
        etf_h = [h for h in all_holdings if h['etf_code'] == etf_code]
        if etf_h:
            total_saved += save_holdings(etf_code, today_str, etf_h, stock_prices)
    log.info(f"✓ 儲存 {total_saved} 筆持股")

    # 補抓 close_price=0 的持股（TPEx 第一次可能 DNS 失敗，備用 URL 補上）
    # 回傳的 prices 合併進 stock_prices，讓 detect_changes 用到完整 222 支
    log.info("補抓缺收盤價的持股...")
    extra_prices = reprice_zero_holdings(today_str)
    if extra_prices:
        stock_prices.update(extra_prices)
        log.info(f"  stock_prices 更新至 {len(stock_prices)} 支")

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

    df_changes = detect_changes(today_str, yesterday_str, prices=stock_prices)

    # 計算連續加碼/減碼天數（streak）
    by_etf_stock, by_stock = compute_streaks(today_str, lookback_days=10)
    export_streaks_json(by_etf_stock, by_stock, today_str)

    # 排行榜
    print_rankings(df_changes, df_today)

    # 匯出 JSON
    export_json(today_str, df_today, df_changes, etf_prices_today, nav_map, aum_map)

    # 匯出 CSV（共同加碼排行前10，給 BT repo 資料庫使用）
    export_etf_csv(today_str, df_changes)

    log.info(f"✅ 完成！{today_str} 共 {len(df_today)} 筆持股")


if __name__ == '__main__':
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else None)
