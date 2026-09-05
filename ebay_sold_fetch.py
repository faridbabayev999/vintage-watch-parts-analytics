"""
ebay_sold_fetch.py  --  STAGE 1: download PUBLIC eBay SOLD (completed) result
pages for Rolex watch parts, itemwise (each sold listing = one row later).

Unlike Terapeak (AGGREGATED per-product data) and unlike Chrono24 (DataDome),
eBay's public sold-listing search has NO heavy bot-wall, so a real browser with
a saved profile collects it for FREE. You only solve the occasional captcha by
hand in the window.

It RESUMES: any page already saved in ./ebay_sold_pages/ is skipped.

Setup (run once):
    pip install playwright
    playwright install chromium

Close all normal Edge windows first, then:
    python ebay_sold_fetch.py

240 items per page. Pages until eBay stops advancing (real end) or END_PAGE.
"""

import asyncio
import random
import re
import pathlib
from playwright.async_api import async_playwright

# ----------------------- settings you can change -----------------------
START_PAGE  = 1
END_PAGE    = 30        # auto-stops earlier at the real end of results
HEADLESS    = False     # MUST stay False so you can solve any captcha
BLOCK_WAITS = 18        # 10s waits allowed on a blocked page (~3 min)
PROFILE_DIR = pathlib.Path("ebay_public_profile")
OUT_DIR     = pathlib.Path("ebay_sold_pages")

# 240 items/page (_ipg=240), Sold+Completed, category 173696 = Watch Parts,
# broad keyword "rolex" so parts without a caliber in the title are included.
URL_TEMPLATE = ("https://www.ebay.de/sch/173696/i.html"
                "?_nkw=rolex&LH_Sold=1&LH_Complete=1&_ipg=240&_pgn={PGN}")
# -----------------------------------------------------------------------

OUT_DIR.mkdir(exist_ok=True)

STEALTH = "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"

ITEM_RE = re.compile(r"/itm/(\d{6,15})")


def item_ids(html: str):
    return ITEM_RE.findall(html)


async def open_browser(p):
    common = dict(
        user_data_dir=str(PROFILE_DIR),
        headless=HEADLESS,
        args=["--disable-blink-features=AutomationControlled"],
        locale="de-DE",
        timezone_id="Europe/Berlin",
        viewport={"width": 1366, "height": 900},
    )
    for channel in ("msedge", "chrome", None):
        try:
            if channel:
                ctx = await p.chromium.launch_persistent_context(channel=channel, **common)
                print("Using real browser channel: " + channel)
            else:
                ctx = await p.chromium.launch_persistent_context(**common)
                print("Using bundled Chromium (Edge/Chrome not found)")
            return ctx
        except Exception:
            continue
    raise RuntimeError("Could not launch any browser.")


async def main():
    async with async_playwright() as p:
        ctx = await open_browser(p)
        await ctx.add_init_script(STEALTH)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        saved = skipped = 0
        prev_first5 = None

        for n in range(START_PAGE, END_PAGE + 1):
            out = OUT_DIR / ("ebay_sold_p%d.html" % n)
            if out.exists() and out.stat().st_size > 60000:
                skipped += 1
                prev_first5 = item_ids(out.read_text(encoding="utf-8"))[:5]
                continue

            url = URL_TEMPLATE.format(PGN=n)
            print("[page %d] %s" % (n, url))
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print("  ! navigation error: %s  (re-run to resume)" % e)
                break

            for label in ["Alle akzeptieren", "Akzeptieren", "Accept all", "Accept"]:
                try:
                    await page.click("button:has-text('%s')" % label, timeout=2000)
                    break
                except Exception:
                    pass

            for _ in range(4):
                await page.mouse.move(random.randint(200, 1100), random.randint(200, 800))
                await page.mouse.wheel(0, 3000)
                await page.wait_for_timeout(600)

            html = await page.content()
            ids = item_ids(html)

            if len(ids) < 5 and len(html) < 60000:
                print("  ! captcha/block - SOLVE IT in the window now (usually once).")
                for w in range(BLOCK_WAITS):
                    await page.wait_for_timeout(10000)
                    html = await page.content()
                    ids = item_ids(html)
                    print("    waiting... (%ds, %d items)" % ((w + 1) * 10, len(ids)))
                    if len(ids) >= 5:
                        print("  resolved - continuing.")
                        break

            if len(ids) < 5:
                print("  still blocked / no items. Stopping so progress is kept.")
                break

            first5 = ids[:5]
            if prev_first5 is not None and first5 == prev_first5:
                print("  same items as previous page - eBay not advancing "
                      "(genuine end of sold results). Stopping.")
                break
            prev_first5 = first5

            out.write_text(html, encoding="utf-8")
            saved += 1
            print("  saved %s  (~%d item links, %d bytes)" % (out, len(ids), len(html)))

            await page.wait_for_timeout(random.randint(8000, 16000))

        await ctx.close()
        print("\nDone this run. Saved %d new page(s), skipped %d." % (saved, skipped))
        total = len(list(OUT_DIR.glob("ebay_sold_p*.html")))
        print("Total pages collected so far: %d" % total)
        print("After page 1 saves, upload ebay_sold_pages/ebay_sold_p1.html "
              "so the parser can be locked to eBay's real markup.")


if __name__ == "__main__":
    asyncio.run(main())
