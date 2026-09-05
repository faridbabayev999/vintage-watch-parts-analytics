"""
ebay_sold_parse.py  --  STAGE 2: turn saved eBay SOLD pages into an itemwise CSV.

Reads every ./ebay_sold_pages/ebay_sold_p*.html and writes ebay_sold_items.csv,
ONE ROW PER SOLD LISTING (itemwise, not aggregated).

    python ebay_sold_parse.py

Selectors confirmed against real ebay.de sold pages (July 2026 markup):
    card      div.s-item-card
    title     .su-item-card__title
    price     .su-item-card__price
    subtitle  .su-item-card__subtitle   ("Gebraucht / Gewerblich")
    sold date span starting "Verkauft"  ("Verkauft 13. Jul 2026")
    shipping  .su-card-container__attributes__primary
    seller    .su-program-badge
"""

import csv
import re
import pathlib
from bs4 import BeautifulSoup

PAGES_DIR = pathlib.Path("ebay_sold_pages")
OUT_CSV = "ebay_sold_items.csv"

MONTHS = {"jan": 1, "feb": 2, "mär": 3, "mar": 3, "mrz": 3, "apr": 4, "mai": 5,
          "jun": 6, "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "dez": 12}

ITM_RE = re.compile(r"/itm/(\d{6,15})")
DATE_RE = re.compile(r"(\d{1,2})\.?\s+([A-Za-zäöü]{3,4})\.?\s+(\d{4})")
MONEY_RE = re.compile(r"(\d[\d.\s]*(?:,\d+)?)")


def money(text):
    if not text:
        return None
    m = MONEY_RE.search(text)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(" ", "").replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def iso_date(text):
    if not text:
        return "", ""
    m = DATE_RE.search(text)
    if not m:
        return "", text.replace("Verkauft", "").strip()
    day, mon, year = int(m.group(1)), m.group(2).lower()[:3], int(m.group(3))
    month = MONTHS.get(mon)
    raw = m.group(0)
    if not month:
        return "", raw
    return "%04d-%02d-%02d" % (year, month, day), raw


def text_of(card, selector):
    el = card.select_one(selector)
    return el.get_text(" ", strip=True) if el else ""


def parse_card(card, source):
    link = card.select_one("a[href*='/itm/']")
    href = link["href"] if link else ""
    m = ITM_RE.search(href)
    item_number = m.group(1) if m else ""
    url = href.split("?")[0]

    title = text_of(card, ".su-item-card__title")
    if not item_number or title.strip().lower() in ("shop on ebay", ""):
        return None

    price_txt = text_of(card, ".su-item-card__price")
    price = money(price_txt)
    currency = "EUR" if "EUR" in price_txt else (
        "USD" if "$" in price_txt or "US" in price_txt else "")

    subtitle = text_of(card, ".su-item-card__subtitle")
    condition, seller_type = "", ""
    if subtitle:
        parts = [p.strip() for p in subtitle.split("·")]
        condition = parts[0] if parts else ""
        seller_type = parts[1] if len(parts) > 1 else ""

    date_el = card.find(lambda t: t.name == "span"
                        and t.get_text(strip=True).startswith("Verkauft"))
    date_src = date_el.get_text(" ", strip=True) if date_el else card.get_text(" ", strip=True)
    sold_iso, sold_raw = iso_date(date_src)
    has_sold_stamp = 1 if date_el is not None else 0

    attrs = [e.get_text(" ", strip=True)
             for e in card.select(".su-card-container__attributes__primary")]
    ship_eur, free_ship = None, 0
    location, best_offer = "", 0
    for a in attrs:
        low = a.lower()
        if "gratis" in low or "kostenlos" in low or "free" in low:
            free_ship, ship_eur = 1, 0.0
        elif "lieferung" in low or "versand" in low:
            v = money(a)
            if v is not None:
                ship_eur = v
        if low.startswith("aus ") or " aus " in low:
            location = a.replace("aus", "").strip()
        if "preisvorschlag" in low or "best offer" in low:
            best_offer = 1

    seller = text_of(card, ".su-program-badge")

    return {
        "item_number": item_number,
        "title": title,
        "price_eur": price if currency == "EUR" else "",
        "currency": currency,
        "condition": condition,
        "seller_type": seller_type,
        "sold_date_iso": sold_iso,
        "sold_date_raw": sold_raw,
        "is_sold": has_sold_stamp,
        "shipping_eur": ship_eur if ship_eur is not None else "",
        "free_shipping": free_ship,
        "best_offer": best_offer,
        "location": location,
        "seller": seller,
        "url": url,
        "source_page": source,
    }


def main():
    pages = sorted(PAGES_DIR.glob("ebay_sold_p*.html"),
                   key=lambda p: int(re.search(r"p(\d+)", p.name).group(1)))
    if not pages:
        print("No HTML in ./ebay_sold_pages/. Run ebay_sold_fetch.py first.")
        return

    rows = []
    for f in pages:
        soup = BeautifulSoup(f.read_text(encoding="utf-8"), "html.parser")
        cards = soup.select("div.s-item-card")
        kept = 0
        for c in cards:
            row = parse_card(c, f.name)
            if row:
                rows.append(row)
                kept += 1
        print("%s: %d cards -> %d listings" % (f.name, len(cards), kept))

    seen, deduped = set(), []
    for r in rows:
        if r["item_number"] in seen:
            continue
        seen.add(r["item_number"])
        deduped.append(r)

    fields = ["item_number", "title", "price_eur", "currency", "condition",
              "seller_type", "sold_date_iso", "sold_date_raw", "is_sold",
              "shipping_eur", "free_shipping", "best_offer", "location",
              "seller", "url", "source_page"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as out:
        w = csv.DictWriter(out, fieldnames=fields)
        w.writeheader()
        w.writerows(deduped)

    not_sold = sum(1 for r in deduped if not r["is_sold"])
    print("\nParsed %d rows -> %d unique sold listings" % (len(rows), len(deduped)))
    print("Rows NOT carrying a 'Verkauft' sold stamp: %d" % not_sold)
    print("Wrote", OUT_CSV)


if __name__ == "__main__":
    main()
