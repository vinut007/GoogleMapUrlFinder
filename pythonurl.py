"""
Google Maps company URL scraper - Playwright (lightweight 50 workers)
ONE browser process + N tab workers => 50 workers ≈ same RAM as 2-3 Edge browsers.
Reads:   508K_Rel_MM Accounts.csv
Writes:  508K_Rel_MM_Accounts_with_urls.csv (all columns + URL + MATCHED)
Resume-safe: rows already in output are skipped.
Usage:
  python map_url_scraper_pw.py                  # 50 workers
  python map_url_scraper_pw.py --workers 10     # 10 workers
  python map_url_scraper_pw.py --limit 20       # test 20 rows
  python map_url_scraper_pw.py --headed         # visible browser
"""
import argparse
import asyncio
import csv
import os
import re
import sys
import time
from urllib.parse import quote, unquote, urlparse

from playwright.async_api import async_playwright

SRC_CSV = r"C:\Users\vinut\Downloads\SCRAPING\NOTurl.csv"
OUT_CSV = r"C:\Users\vinut\Downloads\SCRAPING\508K_Rel_MM_Accounts_with_urls.csv"
SEARCH_URL = "https://www.google.com/maps/search/{}?hl=en"
TIMEOUT_GOTO = 12000
TIMEOUT_ADDR = 6000
POLL_SECONDS = 8
MAX_FEED_CANDIDATES = 3

COUNTRY_ALIASES = {
    "United States": "USA", "United States of America": "USA",
    "U.S.A.": "USA", "U.S.": "USA", "South Korea": "Korea",
    "Russian Federation": "Russia", "Czech Republic": "Czechia",
    "UAE": "United Arab Emirates", "United Arab Emirates (UAE)": "United Arab Emirates",
    "UK": "United Kingdom", "Great Britain": "United Kingdom",
    "Viet Nam": "Vietnam", "Bosnia": "Bosnia and Herzegovina",
    "Ivory Coast": "Cote d'Ivoire", "Cote d Ivoire": "Cote d'Ivoire",
}

done_ids = set()
stats = {"ok": 0, "fail": 0, "matched": {}}
stop_requested = False
CURRENT_TOTAL = 0


def log(msg):
    print(msg, flush=True)


def normalize(s):
    if not s:
        return ""
    s = re.sub(r"[^a-z0-9&.-]+", " ", s.lower().replace("\u00df", "ss"))
    return re.sub(r"\s+", " ", s).strip()


def extract_city(address_text):
    if not address_text:
        return ""
    parts = [p.strip() for p in address_text.split(",") if p.strip()]
    return parts[-2] if len(parts) >= 2 else (parts[-1] if parts else "")


def city_matches(csv_city, addr_text):
    nc = normalize(csv_city)
    if not nc or not addr_text:
        return False
    na = normalize(addr_text)
    if nc in na:
        return True
    short = [w for w in nc.split() if len(w) > 3]
    if short and all(w in na for w in short):
        return True
    addr_city = normalize(extract_city(addr_text))
    if addr_city and (addr_city in nc or nc in addr_city):
        return True
    return False


NAME_STOP_WORDS = {
    "sa", "srl", "ltda", "gmbh", "kg", "inc", "ltd", "llc", "llp", "corp",
    "corporation", "spa", "co", "the", "de", "da", "do", "das", "dos", "e",
    "y", "and", "of", "company", "plc", "pv", "pte", "bhd", "oy", "ab",
    "nv", "bv", "ag", "kk", "sl", "sc", "limited", "private", "group",
    "holding", "sdn", "pt",
}


def significant_words(name):
    if not name:
        return []
    name = re.sub(r"\([^)]*\)", " ", name)
    words = normalize(name).split()
    return [w for w in words if len(w) > 2 and w not in NAME_STOP_WORDS]


def name_matches(csv_name, result_name):
    sig = significant_words(csv_name)
    if not sig:
        return True
    nr = normalize(result_name)
    return all(w in nr for w in sig)


async def get_result_name(page):
    for sel in ["h1.DUwDvf", "h1.fontHeadlineSmall", "h1"]:
        try:
            h1 = page.locator(sel).first
            if await h1.count():
                return await h1.inner_text()
        except Exception:
            pass
    return ""


def country_matches(csv_country, addr_text):
    if not csv_country:
        return True
    if not addr_text:
        return False
    nc = normalize(csv_country)
    if nc == "usa" or nc == "us" or nc == "united states":
        nc = "united states"
    na = normalize(addr_text)
    if nc in na:
        return True
    short = [w for w in nc.split() if len(w) > 3]
    if short and all(w in na for w in short):
        return True
    return False


def clean_company(name):
    if not name:
        return ""
    n = re.sub(r"\b(INC|INCORPORATED|LTD|LIMITED|LLC|LLP|PLC|CORP|CORPORATION|CO|COMPANY|GMBH|GMBH & CO|AG|KG|SA|S\.A\.|SRL|PTY|PVT|LDA|LTDA|OY|OYJ|NV|BV|AB|SL|SPA|SAPA|SARL|KK|CO\.LTD|PTE)\b", "", name, flags=re.IGNORECASE)
    n = re.sub(r"[^\w\s&-]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def build_query(name, city, country):
    parts = []
    cn = clean_company(name)
    if cn:
        parts.append(cn)
    if city:
        parts.append(city)
    if country:
        parts.append(COUNTRY_ALIASES.get(country, country))
    return quote(" ".join(parts))


def decode_url(href):
    if not href:
        return ""
    if "google.com/maps/place" in href and "url=" in href:
        m = re.search(r"[?&]url=([^&]+)", href)
        if m:
            return unquote(m.group(1))
    return href


def clean_url(url):
    if not url:
        return ""
    try:
        p = urlparse(url)
        if p.scheme and p.netloc:
            if p.netloc in ("wa.me", "api.whatsapp.com"):
                return url
            return f"{p.scheme}://{p.netloc}/"
    except Exception:
        pass
    return url


def get_field(row, name):
    if name in row:
        return row[name]
    low = name.lower()
    for k in row:
        if k.lower() == low:
            return row[k]
    return ""


async def poll_place_panel(page):
    """Return address text once the place panel opens (address OR website link
    visible); return None if no place panel appears within POLL_SECONDS."""
    deadline = time.time() + POLL_SECONDS
    while time.time() < deadline:
        try:
            addr = page.locator("button[data-item-id='address']").first
            if await addr.count():
                return await addr.inner_text()
        except Exception:
            pass
        try:
            wl = page.locator("a[data-item-id^='authority']").first
            if await wl.count():
                return ""
        except Exception:
            pass
        await asyncio.sleep(1.0)
    return None


async def process_row(page, row):
    name = (get_field(row, "Company_NAME") or "").strip()
    city = (get_field(row, "CITY") or "").strip()
    country = (get_field(row, "COUNTRY") or "").strip()
    query = build_query(name, city, country)

    try:
        await page.goto(SEARCH_URL.format(query), timeout=TIMEOUT_GOTO, wait_until="domcontentloaded")
    except Exception:
        return None

    addr_text = await poll_place_panel(page)
    result_name = ""

    # no direct place panel -> follow feed candidates (bounded), verifying name on each
    if addr_text is None:
        hrefs = []
        try:
            feed = page.locator("div[role='feed'] a[href*='/maps/place/']")
            n = min(await feed.count(), MAX_FEED_CANDIDATES)
            for i in range(n):
                hrefs.append(await feed.nth(i).get_attribute("href"))
        except Exception:
            pass
        for pu in hrefs:
            try:
                await page.goto(pu, timeout=TIMEOUT_GOTO, wait_until="domcontentloaded")
            except Exception:
                continue
            addr_text = await poll_place_panel(page)
            result_name = await get_result_name(page)
            if name_matches(name, result_name):
                break
    else:
        result_name = await get_result_name(page)

    city_ok = city_matches(city, addr_text) if city else True
    country_ok = country_matches(country, addr_text) if country else True
    name_ok = name_matches(name, result_name)
    if name_ok:
        matched = "YES"
    elif city_ok and city:
        matched = "CITY"
    elif country_ok and country:
        matched = "COUNTRY"
    else:
        matched = "NO"
    url = ""
    try:
        wl = page.locator("a[data-item-id^='authority']").first
        if await wl.count():
            url = clean_url(decode_url(await wl.get_attribute("href")))
    except Exception:
        pass
    if not name_ok:
        url = ""
    row_out = dict(row)
    row_out["URL"] = url
    row_out["MATCHED"] = matched
    row_out["RESULT_NAME"] = result_name[:120]
    return row_out


async def worker(worker_id, queue, out_file, browser):
    ctx = await browser.new_context(locale="en-US")
    page = await ctx.new_page()
    await asyncio.sleep(worker_id * 0.3)
    log(f"Worker {worker_id}: ready")
    processed = 0
    try:
        while True:
            try:
                row = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if stop_requested:
                    break
                continue
            if row is None:
                break
            kid = (get_field(row, "KEYID") or "").strip()
            result = None
            for attempt in range(2):
                try:
                    result = await process_row(page, row)
                    if result:
                        break
                except Exception as e:
                    log(f"  [W{worker_id}] retry {(get_field(row, 'Company_NAME') or '')[:28]}: {str(e)[:50]}")
                    await asyncio.sleep(1.5)
            if result:
                out_file.write(result)
                done_ids.add(kid)
                stats["ok"] += 1
                stats["matched"][result["MATCHED"]] = stats["matched"].get(result["MATCHED"], 0) + 1
                processed += 1
                status = result["MATCHED"]
                log(f"  [W{worker_id}-{processed}] {(get_field(row, 'Company_NAME') or '')[:32]} [{status}] -> {result['URL'][:60] or 'NO URL'}")
            else:
                stats["fail"] += 1
                log(f"  [W{worker_id}] {(get_field(row, 'Company_NAME') or '')[:38]} -> FAILED")
    finally:
        await ctx.close()
    log(f"Worker {worker_id}: done ({processed} saved)")


class SafeWriter:
    def __init__(self, path, fieldnames, delimiter=","):
        self._f = open(path, "w", newline="", encoding="utf-8-sig")
        self._w = csv.writer(self._f, delimiter=delimiter)
        self._w.writerow(fieldnames)
        self._fieldnames = fieldnames

    def write(self, row_dict):
        self._w.writerow([row_dict.get(k, "") for k in self._fieldnames])
        self._f.flush()

    def close(self):
        self._f.close()


def sniff_delim(path, enc):
    with open(path, encoding=enc, errors="replace", newline="") as f:
        sample = f.read(65536)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",\t;|").delimiter
    except Exception:
        return ","


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--count", type=int, default=0)
    ap.add_argument("--workers", type=int, default=50)
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--src", default=SRC_CSV, help="source CSV path")
    ap.add_argument("--out", default=OUT_CSV, help="output CSV path")
    args = ap.parse_args()
    run_scrape(args)


def run_scrape(args):
    src_csv = args.src
    out_csv = args.out
    workers = max(1, min(args.workers, 100))
    log(f"Workers: {workers} (one browser, {workers} tabs)")
    log(f"Source: {src_csv}")

    global done_ids, stats
    done_ids = set()
    stats = {"ok": 0, "fail": 0, "matched": {}}
    if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
        out_delim = sniff_delim(out_csv, "utf-8-sig")
        with open(out_csv, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f, delimiter=out_delim):
                kid = (get_field(r, "KEYID") or "").strip()
                if kid:
                    done_ids.add(kid)
        log(f"Resume: {len(done_ids)} rows already processed")

    src = open(src_csv, "rb")
    head = src.read(65536)
    src.close()
    try:
        head.decode("utf-8")
        enc = "utf-8"
    except UnicodeDecodeError:
        enc = "latin-1"
    delim = sniff_delim(src_csv, enc)
    src = open(src_csv, encoding=enc, errors="replace", newline="")
    reader = csv.DictReader(src, delimiter=delim)

    global CURRENT_TOTAL
    with open(src_csv, encoding=enc, errors="replace", newline="") as cf:
        CURRENT_TOTAL = sum(1 for _ in csv.DictReader(cf, delimiter=delim))
    log(f"Total rows: {CURRENT_TOTAL}")
    out = SafeWriter(out_csv, [k for k in reader.fieldnames if k.strip()] + ["URL"], delim)

    q = asyncio.Queue(maxsize=workers * 2)

    async def feeder():
        total = 0
        for row in reader:
            total += 1
            if args.start and total < args.start:
                continue
            if args.limit and (total - args.start) > args.limit:
                break
            if args.count and (total - args.start) >= args.count:
                break
            kid = (get_field(row, "KEYID") or "").strip()
            if kid and kid in done_ids:
                continue
            if stop_requested:
                break
            await q.put(row)
        for _ in range(workers):
            await q.put(None)

    async def run():
        async with async_playwright() as p:
            browser = None
            try:
                browser = await p.chromium.launch(
                    channel="msedge", headless=not args.headed,
                    args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage",
                          "--disable-blink-features=AutomationControlled",
                          "--disable-extensions", "--disable-background-networking",
                          "--disable-component-update", "--disable-sync", "--mute-audio"]
                )
            except Exception as e:
                log(f"Edge launch failed ({str(e)[:80]}), falling back to bundled Chromium...")
                browser = await p.chromium.launch(
                    headless=not args.headed,
                    args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage",
                          "--disable-blink-features=AutomationControlled",
                          "--disable-extensions", "--disable-background-networking",
                          "--disable-component-update", "--disable-sync", "--mute-audio"]
                )
            tasks = [asyncio.create_task(worker(i + 1, q, out, browser)) for i in range(workers)]
            feed_task = asyncio.create_task(feeder())
            await asyncio.gather(feed_task, *tasks)
            await browser.close()

    asyncio.run(run())

    src.close()
    out.close()
    log(f"\nDONE. OK: {stats['ok']}, Failed: {stats['fail']}, Total saved: {len(done_ids)}")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    main()
