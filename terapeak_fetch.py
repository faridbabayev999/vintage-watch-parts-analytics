"""
terapeak_fetch.py  --  collect Terapeak SOLD pages, with pagination.

For each search term it walks the result PAGES via the URL's offset parameter
(0, 50, 100, ...) until a page returns fewer than 50 rows (= last page).

RESUME: every page is saved as its own file; pages already saved are skipped,
so re-running only fetches what's missing. Stop anytime with Ctrl+C.

Run:
    python terapeak_fetch.py
Then:
    python terapeak_parse.py
"""

import asyncio
import pathlib
import random
import re
import urllib.parse
from playwright.async_api import async_playwright

# Your Terapeak SOLD URL, keyword as {KW}, offset as {OFF}.
URL_TEMPLATE = (
    "https://www.ebay.de/sh/research?marketplace=EBAY-DE&keywords={KW}"
    "&dayRange=1095&endDate=1781602803040&startDate=1686994803040"
    "&categoryId=10324&offset={OFF}&limit=50&tabName=SOLD&tz=Europe%2FBerlin"
)

KEYWORDS = [
    # Broad sweep: grab everything in the parts category, then filter later
    # in terapeak_clean.py (genuine/generic + caliber/part-number extraction).
    # Several broad terms so that, if Terapeak caps each search, the union still
    # covers a lot. Dedup by item number happens in the cleaning step.
    "Rolex",
    "Rolex Ersatzteil",
    "Rolex part",
    "Rolex parts",
    "Rolex original",
    "Rolex genuine",
    "Rolex NOS",
    "Rolex caliber",
    "Rolex Werk",
    "Rolex movement",
]

HEADLESS    = False
PROFILE_DIR = pathlib.Path("ebay_profile")
OUT_DIR     = pathlib.Path("terapeak_pages")
LOGIN_WAITS = 24
LIMIT       = 50
MAX_PAGES   = 200
ROW_MARKER  = "research-table-row__product-info-name"

OUT_DIR.mkdir(exist_ok=True)


def slug(kw): return re.sub(r"[^a-z0-9]+", "_", kw.lower()).strip("_")
def url_for(kw, off): return URL_TEMPLATE.format(KW=urllib.parse.quote(kw), OFF=off)
def count_rows(html): return html.count(ROW_MARKER)
def signature(html): return tuple(re.findall(r"/itm/(\d{6,15})", html)[:5])


async def safe_content(page, tries=4):
    for _ in range(tries):
        try:
            return await page.content()
        except Exception:
            await page.wait_for_timeout(1500)
    return ""


async def open_browser(p):
    common = dict(user_data_dir=str(PROFILE_DIR), headless=HEADLESS,
                  args=["--disable-blink-features=AutomationControlled"],
                  locale="de-DE", timezone_id="Europe/Berlin",
                  viewport={"width": 1366, "height": 900})
    for channel in ("msedge", "chrome", None):
        try:
            return await (p.chromium.launch_persistent_context(channel=channel, **common)
                          if channel else p.chromium.launch_persistent_context(**common))
        except Exception:
            continue
    raise RuntimeError("Could not launch a browser.")


async def fetch_page(page, kw, off):
    await page.goto(url_for(kw, off), wait_until="domcontentloaded", timeout=60000)
    try:
        await page.wait_for_selector(f".{ROW_MARKER}", timeout=20000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)
    return await safe_content(page)


async def main():
    async with async_playwright() as p:
        ctx = await open_browser(p)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        await page.goto(url_for("Rolex", 0), wait_until="domcontentloaded", timeout=60000)
        for label in ["Akzeptieren", "Alle akzeptieren", "Accept", "Zustimmen"]:
            try:
                await page.click(f"button:has-text('{label}')", timeout=2000); break
            except Exception:
                pass

        async def needs_login():
            try:
                return (await page.query_selector("input[type='password']")) is not None
            except Exception:
                return False

        if await needs_login():
            print("eBay login screen detected - LOG IN now in the window. Waiting...")
            for w in range(LOGIN_WAITS):
                await page.wait_for_timeout(10000)
                if not await needs_login():
                    print("  logged in - continuing."); break
                print(f"    waiting for login... ({(w+1)*10}s)")
        else:
            print("Already logged in - starting collection.")

        saved = skipped = 0
        for kw in KEYWORDS:
            prev_sig = None
            for pidx in range(1, MAX_PAGES + 1):
                off = (pidx - 1) * LIMIT
                out = OUT_DIR / f"terapeak_{slug(kw)}_p{pidx}.html"

                if out.exists():
                    have = out.read_text(encoding="utf-8", errors="ignore")
                    rows = count_rows(have)
                    if rows > 0:
                        skipped += 1
                        prev_sig = signature(have)
                        if rows < LIMIT:
                            break
                        continue

                print(f"[{kw}] page {pidx} (offset {off})...")
                html = await fetch_page(page, kw, off)
                rows = count_rows(html)
                sig = signature(html)

                if rows == 0:
                    if pidx == 1:
                        print(f"    -> no sold listings for '{kw}'.")
                    else:
                        print(f"    -> page {pidx} empty: no more results for '{kw}'.")
                    break
                if sig and sig == prev_sig:
                    print(f"    -> page {pidx} is identical to the previous page: Terapeak ignored the offset for '{kw}'. Stopping this term.")
                    break

                out.write_text(html, encoding="utf-8")
                saved += 1
                prev_sig = sig
                print(f"  saved {out.name}  ({rows} rows)")
                if rows < LIMIT:
                    print(f"    -> {rows} < {LIMIT}: last page for '{kw}'. Done.")
                    break
                print(f"    -> {rows} rows (full page): trying page {pidx + 1}...")
                await page.wait_for_timeout(random.randint(4000, 8000))

        await ctx.close()
        total = len(list(OUT_DIR.glob("terapeak_*.html")))
        print(f"\nDone. Saved {saved} new pages, skipped {skipped} already-have. Total files: {total}")
        print("Next: python terapeak_parse.py")


if __name__ == "__main__":
    asyncio.run(main())
