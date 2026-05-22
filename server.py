#!/usr/bin/env python3
"""
Canadian Deals Server
Sources:
  • Walmart.ca     — flash deals + rollbacks (6 pages each, ~480 deals)
  • StockTrack.ca  — price drops (Staples, Home Depot, Rona, Best Buy…)
Features:
  • /api/deals   — full deal list (JSON)
  • /api/status  — lightweight: count + fetch timestamp
  • /api/events  — Server-Sent Events stream (new deals → browser toast)
  • /            — HTML dashboard
"""

import json
import secrets as _secrets_mod
import urllib.request
import urllib.parse
import gzip
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import html as html_mod
import threading
import time
import hashlib
import sqlite3
import os
import smtplib
import xml.etree.ElementTree as ET
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from datetime import datetime

PORT = int(os.environ.get("PORT", "8080"))
DB_PATH = os.path.join(os.path.dirname(__file__), "price_history.db")
_db_lock = threading.Lock()

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")

# ─────────────────────────────────────────────────────────
# Price history (SQLite)
# ─────────────────────────────────────────────────────────
def _init_db():
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""CREATE TABLE IF NOT EXISTS price_min (
            tid       TEXT PRIMARY KEY,
            store     TEXT,
            title     TEXT,
            min_price REAL,
            first_seen TEXT,
            last_seen  TEXT,
            count     INTEGER DEFAULT 1
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS price_points (
            tid      TEXT,
            price    REAL,
            seen_at  TEXT,
            PRIMARY KEY (tid, seen_at)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            email        TEXT NOT NULL,
            tid          TEXT NOT NULL,
            store        TEXT,
            title        TEXT,
            target_price REAL,
            current_price REAL,
            link         TEXT,
            created_at   TEXT,
            fired_at     TEXT
        )""")
        conn.commit()

_init_db()


def _parse_price_float(price_str):
    if not price_str:
        return None
    m = re.search(r'\$\s*(\d[\d,]*(?:\.\d{1,2})?)', str(price_str))
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except Exception:
            return None
    return None


def _update_price_history(all_deals):
    """Annotate each deal with isLowest=True/False based on SQLite history."""
    now = datetime.utcnow().isoformat()
    try:
        with _db_lock:
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                cur = conn.cursor()
                for d in all_deals:
                    price = _parse_price_float(d.get("currentPrice", ""))
                    if price is None or price <= 0:
                        d["isLowest"] = False
                        continue
                    tid = d["tid"]
                    cur.execute("SELECT min_price FROM price_min WHERE tid=?", (tid,))
                    row = cur.fetchone()
                    if row is None:
                        cur.execute(
                            "INSERT INTO price_min VALUES (?,?,?,?,?,?,1)",
                            (tid, d.get("store",""), d.get("title","")[:120],
                             price, now, now)
                        )
                        cur.execute("INSERT OR IGNORE INTO price_points VALUES (?,?,?)",
                                    (tid, price, now[:10]))
                        d["isLowest"] = True
                    elif price <= row[0]:
                        if price < row[0]:
                            cur.execute(
                                "UPDATE price_min SET min_price=?,last_seen=?,count=count+1 WHERE tid=?",
                                (price, now, tid)
                            )
                        else:
                            cur.execute(
                                "UPDATE price_min SET last_seen=?,count=count+1 WHERE tid=?",
                                (now, tid)
                            )
                        # Record a new point only if price differs from last recorded
                        last = cur.execute(
                            "SELECT price FROM price_points WHERE tid=? ORDER BY seen_at DESC LIMIT 1",
                            (tid,)
                        ).fetchone()
                        if not last or last[0] != price:
                            cur.execute("INSERT OR IGNORE INTO price_points VALUES (?,?,?)",
                                        (tid, price, now[:10]))
                        d["isLowest"] = True
                    else:
                        cur.execute(
                            "UPDATE price_min SET last_seen=?,count=count+1 WHERE tid=?",
                            (now, tid)
                        )
                        last = cur.execute(
                            "SELECT price FROM price_points WHERE tid=? ORDER BY seen_at DESC LIMIT 1",
                            (tid,)
                        ).fetchone()
                        if not last or last[0] != price:
                            cur.execute("INSERT OR IGNORE INTO price_points VALUES (?,?,?)",
                                        (tid, price, now[:10]))
                        d["isLowest"] = False
                conn.commit()
    except Exception as e:
        print(f"[price_history] {e}")
        for d in all_deals:
            d.setdefault("isLowest", False)


# ─────────────────────────────────────────────────────────
# Deal deduplication
# ─────────────────────────────────────────────────────────
_STOP = {"the","a","an","and","with","for","in","of","to","&","x"}

def _normalize_title(title):
    t = re.sub(r"[^a-z0-9\s]", " ", title.lower())
    words = [w for w in t.split() if w not in _STOP and len(w) > 1]
    return " ".join(words[:9])


def _deduplicate(deals):
    seen: dict = {}  # key → index in out
    out = []
    for d in deals:
        key = f"{d['store'].strip().lower()}::{_normalize_title(d['title'])}"
        if key not in seen:
            seen[key] = len(out)
            out.append(d)
        else:
            # Keep the deal with the lower current price
            idx = seen[key]
            existing_p = _parse_price_float(out[idx].get("currentPrice", ""))
            new_p = _parse_price_float(d.get("currentPrice", ""))
            if new_p is not None and (existing_p is None or new_p < existing_p):
                out[idx] = d
    return out


# ─────────────────────────────────────────────────────────
# Email alerts
# ─────────────────────────────────────────────────────────
def _send_email(to_addr, subject, body_html):
    if not SMTP_USER or not SMTP_PASS:
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = SMTP_USER
        msg["To"] = to_addr
        msg.attach(MIMEText(body_html, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.ehlo()
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, to_addr, msg.as_string())
    except Exception as e:
        print(f"[email] {e}")


def _check_alerts(all_deals):
    """Fire any price alerts whose target price has been reached."""
    if not SMTP_USER:
        return
    deal_by_tid = {d["tid"]: d for d in all_deals}
    try:
        with _db_lock:
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                rows = conn.execute(
                    "SELECT id,email,tid,store,title,target_price,link "
                    "FROM alerts WHERE fired_at IS NULL"
                ).fetchall()
                now = datetime.utcnow().isoformat()
                for row in rows:
                    aid, email, tid, store, title, target, link = row
                    deal = deal_by_tid.get(tid)
                    if not deal:
                        continue
                    price = _parse_price_float(deal.get("currentPrice",""))
                    if price is None or price > target:
                        continue
                    # Target hit — fire the alert
                    html = f"""<h2>Price Alert: {title}</h2>
<p>{store} — now <strong>${price:.2f}</strong> (your target: ${target:.2f})</p>
<p><a href="{link or '#'}">View deal →</a></p>
<p style="color:#999;font-size:12px">QC &amp; ON Deals alert — reply to unsubscribe.</p>"""
                    _send_email(email, f"Price drop: {title[:60]}", html)
                    conn.execute(
                        "UPDATE alerts SET fired_at=? WHERE id=?", (now, aid)
                    )
                conn.commit()
    except Exception as e:
        print(f"[alerts] {e}")


# ─────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────
_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.google.ca/",
}
_JSON_H = {**_BROWSER,
           "Accept": "application/json, */*",
           "X-Requested-With": "XMLHttpRequest"}


def _get(url, extra=None):
    h = dict(_BROWSER)
    if extra:
        h.update(extra)
    req = urllib.request.Request(url, headers=h)
    last_exc = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read()
            try:
                return gzip.decompress(raw).decode("utf-8", errors="replace")
            except Exception:
                return raw.decode("utf-8", errors="replace")
        except Exception as e:
            last_exc = e
            if attempt == 0:
                time.sleep(2)
    raise last_exc


def _get_json(url, referer="https://stocktrack.ca/"):
    h = dict(_JSON_H)
    h["Referer"] = referer
    req = urllib.request.Request(url, headers=h)
    last_exc = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read()
            try:
                raw = gzip.decompress(raw)
            except Exception:
                pass
            return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as e:
            last_exc = e
            if attempt == 0:
                time.sleep(2)
    raise last_exc


def _clean(s):
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = html_mod.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def _pfmt(v):
    v = str(v or "").strip().lstrip("$")
    return ("$" + v) if v else ""


def _save_pct(cur, was):
    try:
        c = float(re.sub(r"[^\d.]", "", str(cur)))
        w = float(re.sub(r"[^\d.]", "", str(was)))
        if w > 0 and c < w:
            return str(round((w - c) / w * 100)) + "%"
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────────────────
# SOURCE 1: Walmart.ca
# ─────────────────────────────────────────────────────────
# (shelf_id, label, max pages to fetch)
WALMART_FEEDS = [
    ("1984137688763", "Flash Deals", "special_offers%3AReduced+Price", 6),
    ("6000205319047", "Rollbacks",   "",                               5),
]


def _parse_walmart_page(url):
    text = _get(url)
    nd = re.search(r'id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', text, re.S)
    if not nd:
        return []
    try:
        data = json.loads(nd.group(1))
    except Exception:
        return []

    stacks = (data.get("props", {})
                  .get("pageProps", {})
                  .get("initialData", {})
                  .get("searchResult", {})
                  .get("itemStacks", []))

    deals = []
    for stack in stacks:
        for item in stack.get("items", []):
            if item.get("__typename") != "Product":
                continue
            # Skip third-party marketplace sellers — keep only Walmart's own inventory
            if item.get("sellerType") == "EXTERNAL":
                continue
            seller_name = (item.get("sellerName") or "").strip().lower()
            if seller_name and seller_name not in ("walmart.ca", "walmart", ""):
                continue
            name = _clean(item.get("name", ""))
            if not name:
                continue

            pi          = item.get("priceInfo", {}) or {}
            cur_price   = pi.get("linePrice", "") or pi.get("itemPrice", "")
            was_price   = pi.get("wasPrice", "")
            savings_str = pi.get("savings", "")

            # Only include items with a real sale price
            if not cur_price:
                continue

            save_pct = _save_pct(cur_price, was_price)
            if not save_pct and savings_str and cur_price:
                sav_m = re.search(r'\$?\s*(\d[\d,]*\.?\d*)', str(savings_str))
                if sav_m:
                    try:
                        sav = float(sav_m.group(1).replace(',', ''))
                        cur = float(re.sub(r'[^\d.]', '', str(cur_price)))
                        if sav > 0 and cur > 0:
                            save_pct = str(round(sav / (cur + sav) * 100)) + '%'
                    except Exception:
                        pass

            img_info = item.get("imageInfo", {}) or {}
            img = img_info.get("thumbnailUrl", "")

            slug = (item.get("canonicalUrl", "") or "").split("?")[0]
            link = ("https://www.walmart.ca" + slug) if slug else ""

            brand = _clean(item.get("brand", ""))

            cat_path = item.get("category", {}) or {}
            cats = [p.get("name", "") for p in cat_path.get("path", []) if p.get("name")]
            category = " > ".join(cats[:2]) if cats else ""

            deals.append({
                "source":        "Walmart",
                "tid":           item.get("id", "") or item.get("usItemId", ""),
                "title":         name,
                "brand":         brand,
                "store":         "Walmart",
                "link":          link,
                "currentPrice":  cur_price,
                "originalPrice": was_price,
                "savings":       savings_str,
                "savePct":       save_pct,
                "pubDate":       "",
                "relTime":       "",
                "votes":         0,
                "img":           img,
                "category":      category,
                "clearance":     False,
                "dropDate":      "",
                "validUntil":    "",
                "provinces":     ["QC", "ON"],
            })
    return deals


def get_walmart_deals():
    all_deals, errors = [], []
    seen = set()

    for shelf_id, label, facet, max_pages in WALMART_FEEDS:
        base = f"https://www.walmart.ca/en/shop/{shelf_id.lower()}/{shelf_id}"
        if facet:
            base += f"?facet={facet}"

        def _fetch(page, _base=base, _facet=facet):
            sep = "&" if _facet else "?"
            url = _base + (f"{sep}page={page}" if page > 1 else "")
            try:
                return _parse_walmart_page(url)
            except Exception as e:
                return []

        with ThreadPoolExecutor(max_workers=5) as ex:
            pages = list(ex.map(_fetch, range(1, max_pages + 1)))

        for page_deals in pages:
            for d in page_deals:
                if d["tid"] and d["tid"] not in seen:
                    seen.add(d["tid"])
                    d["relTime"] = label
                    d["clearance"] = label == "Clearance"
                    all_deals.append(d)

    return all_deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 2: StockTrack.ca  (price drops JSON API)
# ─────────────────────────────────────────────────────────
ST_BASE = "https://stocktrack.ca"
ST_STORES = {
    "st":   ("Staples",              "https://www.staples.ca/products/{href}"),
    "hd":   ("Home Depot",           "https://www.homedepot.ca{href}"),
    "rona": ("Rona",                 "https://www.rona.ca{href}"),
    "bb":   ("Best Buy",             "https://www.bestbuy.ca/en-ca/product/{href}"),
    "ct":   ("Canadian Tire",        "https://www.canadiantire.ca{href}"),
    "lws":  ("Lowe's",               "https://www.lowes.ca{href}"),
    "sc":   ("Sport Chek",           "https://www.sportchek.ca/products/{href}.html"),
    "pa":   ("Princess Auto",        "https://www.princessauto.com/en/detail/{href}"),
    "dec":  ("Decathlon",            "https://www.decathlon.ca/en/p/{href}"),
    "tru":  ("Toys R Us",            "https://www.toysrus.ca/en/{href}"),
    "ikea": ("IKEA",                 "https://www.ikea.com/ca/en/{href}"),
    "sdm":  ("Shoppers Drug Mart",   "https://www.shoppersdrugmart.ca/en/p/{href}"),
    "src":  ("The Source",           "https://www.thesource.ca/en-ca/product/{href}"),
    "as":   ("Atmosphere",           "https://www.atmosphere.ca/en/p/{href}.html"),
}


def get_stocktrack_deals():
    def _fetch_store(entry):
        sid, (sname, url_tmpl) = entry
        url = (f"{ST_BASE}/{sid}/drops_data.php"
               "?t=all&sort=date&dir=desc&oos=false")
        store_deals, err = [], None
        try:
            data = _get_json(url, referer=f"{ST_BASE}/{sid}/index.php")
            for item in data.get("data", []):
                title = _clean(item.get("name") or item.get("Name") or "")
                if not title:
                    continue
                new_p = str(item.get("newPrice") or item.get("NewPrice") or "").strip()
                old_p = str(item.get("oldPrice") or item.get("OldPrice") or "").strip()
                save  = str(item.get("save")     or item.get("Save")     or "").strip()
                imgs  = item.get("images", [])
                img   = (imgs[0].get("url", "") if isinstance(imgs, list) and imgs
                         else item.get("Image") or "")
                href  = (item.get("url") or item.get("Href") or item.get("href") or "")
                cat   = (item.get("category") or item.get("Category") or "")
                link  = url_tmpl.format(href=href) if href else ""
                store_deals.append({
                    "source":        "StockTrack",
                    "tid":           f"st-{sid}-{item.get('id', '')}",
                    "title":         title,
                    "brand":         "",
                    "store":         sname,
                    "link":          link,
                    "currentPrice":  _pfmt(new_p),
                    "originalPrice": _pfmt(old_p),
                    "savings":       "",
                    "savePct":       (save + "%" if save else ""),
                    "pubDate":       "",
                    "relTime":       "Price Drop",
                    "votes":         0,
                    "img":           img,
                    "category":      cat,
                    "clearance":     False,
                    "dropDate":      "",
                    "validUntil":    "",
                    "provinces":     _deal_provinces(sname),
                })
        except Exception as e:
            err = f"StockTrack {sid}: {e}"
        return store_deals, err

    all_deals, errors = [], []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for store_deals, err in ex.map(_fetch_store, ST_STORES.items()):
            all_deals.extend(store_deals)
            if err:
                errors.append(err)
    return all_deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 3: RedFlagDeals.com  (hot deals forum — HTML scrape)
# ─────────────────────────────────────────────────────────
_RFD_BASE = "https://forums.redflagdeals.com"


def _rfd_clean_store(raw):
    """Normalize dealer name to a known store or 'RedFlagDeals'."""
    known = [
        "Amazon", "Walmart", "Costco", "Best Buy", "Canadian Tire",
        "Staples", "Home Depot", "IKEA", "Sport Chek", "Lowe's",
        "Rona", "Winners", "H&M", "Old Navy", "Gap", "The Source",
        "Shoppers Drug Mart", "London Drugs", "Sobeys", "Metro",
        "Princess Auto", "Decathlon", "Atmosphere", "Reitmans",
        "La Maison Simons", "Simons", "Bureau en Gros",
    ]
    s = raw.strip().lower()
    for k in known:
        if k.lower() in s or s in k.lower():
            return k
    title = raw.strip().title()
    return title if title else "RedFlagDeals"


def get_redflagdeals():
    deals, errors = [], []
    try:
        text = _get(f"{_RFD_BASE}/hot-deals-f9/")
        # Each deal card: <a class="topic-card-info thread_info ..." href="/SLUG/" ...>...</a>
        cards = re.findall(
            r'<a class="topic-card-info thread_info[^"]*"\s+href="(/[^"]+/)"\s+[^>]*'
            r'data-dealer-name=([^\s>]+)([^>]*)>(.*?)</a>',
            text, re.S
        )
        for href, dealer_raw, extra_attrs, content in cards:
            title_m = re.search(r'<h3[^>]*class=thread_title[^>]*>(.*?)</h3>', content, re.S)
            if not title_m:
                continue
            title = _clean(title_m.group(1))
            if not title:
                continue

            dealer_m = re.search(r'<div[^>]*class="dealer_name[^"]*"[^>]*>\s*(.*?)\s*</div>', content, re.S)
            dealer_txt = re.sub(r'<[^>]+>', '', dealer_m.group(1)).strip() if dealer_m else dealer_raw
            store = _rfd_clean_store(dealer_txt or dealer_raw)

            img_m = re.search(r'<img[^>]+src="([^"]+)"', content)
            img = img_m.group(1) if img_m else ""

            votes_m = re.search(r'class="votes[^"]*"[^>]*>.*?</svg>\s*(-?\d+)', content, re.S)
            votes = int(votes_m.group(1)) if votes_m else 0

            dt_m = re.search(r'<time[^>]+datetime="([^"]+)"', content)
            drop_date = (dt_m.group(1) or "")[:10] if dt_m else ""

            link = _RFD_BASE + href

            # If the card exposes a direct merchant URL (e.g. data-goto="https://..."), use it
            goto_m = re.search(r'data-goto="(https?://[^"]+)"', extra_attrs)
            direct_link = goto_m.group(1) if goto_m else ""

            pm = re.search(r'\$\s*(\d[\d,]*(?:\.\d{1,2})?)', title)
            price = f"${pm.group(1)}" if pm else ""
            pct_m = re.search(r'(\d+)\s*%\s*off', title, re.I)
            save_pct = f"{pct_m.group(1)}%" if pct_m else ""
            is_clearance = bool(re.search(r'\bclearance\b', title, re.I))

            tid = "rfd-" + hashlib.md5(href.encode()).hexdigest()[:10]
            deals.append({
                "source":        "RedFlagDeals",
                "tid":           tid,
                "title":         title,
                "brand":         "",
                "store":         store,
                "link":          link,
                "direct_link":   direct_link,
                "currentPrice":  price,
                "originalPrice": "",
                "savings":       "",
                "savePct":       save_pct,
                "pubDate":       drop_date,
                "relTime":       "Hot Deal",
                "votes":         votes,
                "img":           img,
                "category":      "",
                "clearance":     is_clearance,
                "dropDate":      drop_date,
                "validUntil":    "",
                "provinces":     _deal_provinces(store),
            })
    except Exception as e:
        errors.append(f"RedFlagDeals: {e}")
    return deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 5: Shopify retailers (sale collections)
# (store_name, domain, store_category)
# ─────────────────────────────────────────────────────────
_SHOPIFY_STORES = [
    # Clothing — existing
    ("Reitmans",        "www.reitmans.com",        "Clothing"),
    ("Penningtons",     "penningtons.com",          "Clothing"),
    ("Laura",           "www.laura.ca",             "Clothing"),
    ("Melanie Lyne",    "www.melanielyne.com",      "Clothing"),
    ("Thyme Maternity", "thymematernity.com",       "Clothing"),
    # Clothing — new
    ("Ardene",          "www.ardene.com",           "Clothing"),
    ("Frank and Oak",   "www.frankandoak.com",      "Clothing"),
    ("Addition Elle",   "www.additionelle.com",     "Clothing"),
]

# Try these collection slugs in order until one returns products
_SHOPIFY_SALE_SLUGS = ("sale", "clearance", "on-sale", "promotions")


def get_shopify_deals():
    def _fetch_store(store_info):
        store_name, domain, category = store_info
        store_deals, store_errs = [], []
        local_seen = set()
        got_products = False
        for slug in _SHOPIFY_SALE_SLUGS:
            if got_products:
                break
            for page in range(1, 4):
                url = (f"https://{domain}/collections/{slug}/products.json"
                       f"?limit=250&page={page}")
                try:
                    data = _get_json(url, referer=f"https://{domain}/")
                    products = data.get("products", [])
                    if not products:
                        break
                    got_products = True
                    for p in products:
                        handle = p.get("handle", "").strip()
                        title  = _clean(p.get("title", ""))
                        brand  = _clean(p.get("vendor", ""))
                        if not title or not handle:
                            continue
                        imgs = p.get("images", [])
                        img  = imgs[0].get("src", "") if imgs else ""
                        for v in p.get("variants", []):
                            try:
                                cur = float(v.get("price") or 0)
                                old = float(v.get("compare_at_price") or 0)
                            except (TypeError, ValueError):
                                continue
                            if cur <= 0 or old <= cur:
                                continue
                            tid = f"shopify-{domain}-{handle}-{v.get('id','')}"
                            if tid in local_seen:
                                continue
                            local_seen.add(tid)
                            save_amt = round(old - cur, 2)
                            save_pct = str(round((old - cur) / old * 100)) + "%"
                            store_deals.append({
                                "source":        "Shopify",
                                "tid":           tid,
                                "title":         title,
                                "brand":         brand,
                                "store":         store_name,
                                "link":          f"https://{domain}/products/{handle}",
                                "currentPrice":  f"${cur:.2f}",
                                "originalPrice": f"${old:.2f}",
                                "savings":       f"Save ${save_amt:.2f}",
                                "savePct":       save_pct,
                                "pubDate":       "",
                                "relTime":       "Sale",
                                "votes":         0,
                                "img":           img,
                                "category":      category,
                                "clearance":     False,
                                "dropDate":      "",
                                "validUntil":    "",
                                "provinces":     _deal_provinces(store_name),
                            })
                            break
                except Exception as e:
                    store_errs.append(f"Shopify {store_name}/{slug} p{page}: {e}")
                    break
        return store_deals, store_errs

    deals, errors = [], []
    seen = set()
    with ThreadPoolExecutor(max_workers=6) as ex:
        for store_deals, store_errs in ex.map(_fetch_store, _SHOPIFY_STORES):
            for d in store_deals:
                if d["tid"] not in seen:
                    seen.add(d["tid"])
                    deals.append(d)
            errors.extend(store_errs)
    return deals, errors


# ─────────────────────────────────────────────────────────
# Combined fetch + background cache
# ─────────────────────────────────────────────────────────
_cache_lock    = threading.Lock()
_cached_result = None
_sse_clients   = []
_sse_lock      = threading.Lock()
_health_report   = {}
_scout_report    = {}
_pending_command = {"status": "no_command"}
_team_board      = []   # shared message channel between agents
_reports_lock    = threading.Lock()
_click_counts   = {}
_click_lock     = threading.Lock()

def _load_env_password():
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    try:
        for line in open(env_file):
            line = line.strip()
            if line.startswith("MONITOR_PASSWORD="):
                return line.split("=", 1)[1]
    except FileNotFoundError:
        pass
    return os.environ.get("MONITOR_PASSWORD", "")

_monitor_password = _load_env_password()
_admin_token      = _secrets_mod.token_hex(32)

# ── Rate limiter ──
_rl_lock    = threading.Lock()
_rl_counts  = {}   # ip → [count, window_start]
_RL_LIMIT   = 12   # requests per window
_RL_WINDOW  = 60   # seconds

def _rate_ok(ip):
    now = time.time()
    with _rl_lock:
        entry = _rl_counts.get(ip)
        if entry is None or now - entry[1] > _RL_WINDOW:
            _rl_counts[ip] = [1, now]
            return True
        if entry[0] >= _RL_LIMIT:
            return False
        entry[0] += 1
        return True


def _build_and_cache(shared):
    """shared: dict of key → (deals, errs). Updates cache in place."""
    global _cached_result
    all_deals, all_errs = [], []
    counts = {}
    for key, (deals, errs) in shared.items():
        all_deals.extend(deals)
        all_errs.extend(errs)
        counts[key] = len(deals)

    SOURCE_PRIORITY = {"Walmart": 0, "StockTrack": 0, "RedFlagDeals": 0, "Flipp": 1}

    def sort_key(d):
        pct = 0
        if d["savePct"]:
            try:
                pct = float(d["savePct"].rstrip("%"))
            except Exception:
                pass
        return (SOURCE_PRIORITY.get(d["source"], 1), -pct, d["source"], d["store"])

    all_deals.sort(key=sort_key)

    all_deals = _deduplicate(all_deals)
    _update_price_history(all_deals)

    store_counts = {}
    for d in all_deals:
        d["storeCategory"] = _store_category(d["store"])
        store_counts[d["store"]] = store_counts.get(d["store"], 0) + 1

    result = {
        "deals":        all_deals,
        "count":        len(all_deals),
        "walmart":      counts.get("walmart", 0),
        "stocktrack":   counts.get("stocktrack", 0),
        "redflagdeals": counts.get("redflagdeals", 0),
        "flipp":        counts.get("flipp", 0),
        "homedepot":    counts.get("homedepot", 0),
        "rona":         counts.get("rona", 0),
        "lcbo":         counts.get("lcbo", 0),
        "saq":          counts.get("saq", 0),
        "storeCounts":  store_counts,
        "errors":       all_errs,
        "fetched":      datetime.utcnow().isoformat() + "Z",
    }
    with _cache_lock:
        _cached_result = result
    return result


def _fingerprint(result):
    key = ",".join(d["tid"] for d in result["deals"][:200])
    return hashlib.md5(key.encode()).hexdigest()


def _push_sse(payload_str):
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.append(payload_str)
            except Exception:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


# ─────────────────────────────────────────────────────────
# Province mapping — QC-only and ON-only stores
# Anything not listed here is treated as both QC + ON
# ─────────────────────────────────────────────────────────
_QC_ONLY_STORES = {
    "Maxi", "Super C", "Pharmaprix", "Jean Coutu", "SAQ", "Familiprix",
    "Brunet", "Uniprix", "Proxim", "Accès pharma", "Proximed", "Uniprix Sante",
    "Ki Nature & Santé", "Panier Santé La Prairie",
    "IGA", "Provigo", "Bain Depot",
    "Canac", "BMR", "Patrick Morin", "Club Piscine", "Dormez-Vous",
    "Adonis", "Rachelle Béry", "PA Nature", "Avril Supermarché Santé",
    "Mayrand", "Marché Richelieu", "Les Marchés Tradition", "Bureau en gros",
    "Tanguay", "Centre Hi-Fi", "Sports Experts", "Rossy",
    "SAIL", "SAQ",
    "Marche C & T", "Marché Newon", "Marche Nuvo Market", "Grand Marché Col-Fax",
    "Marché Salaberry", "Marché Ami", "Marché Sheng Tai", "Marché Lian Tai",
    "Euro Marche", "Supermarche Aures", "MarchesTAU", "L'Inter-Marché International",
    "Vie En Vert", "Pasquier", "Bonanza", "Supermarche PA", "Kim Phat",
    "Marché Fu Tai", "Val-Mont", "Proxi", "Aubut", "Intermarche Palumbo",
    "Marché Bonichoix", "Mondou", "Animo Etc.", "Chico",
}

_ON_ONLY_STORES = {
    "Shoppers Drug Mart", "Rexall", "No Frills", "Food Basics", "FreshCo",
    "Loblaws", "Fortinos", "Farm Boy", "Longos", "Foodland",
    "T&T Supermarket", "Real Canadian Superstore", "Your Independent Grocer",
    "Highland Farms", "LCBO", "The Beer Store", "Guardian",
    "Pharmasave", "I.D.A.", "Chalo FreshCo", "Sobeys",
    "Valu-Mart", "Independent City Market", "Nations Fresh Foods",
    "Btrust Supermarket", "H Mart",
    "Fiesta Farms", "Lady York Foods", "Oceans Fresh Food Market",
    "Galleria Supermarket", "Foody Mart", "Foody World",
    "Seasons Food Mart", "Cataldi", "Terra Foodmart", "Fresh Land Supermarket",
    "Fusion Supermarket", "Starsky", "A1 Cash + Carry", "Bestco Foodmart",
    "Blue Sky Supermarket", "Danforth Food Market", "Pat Mart",
    "Tone Tai Supermarket", "Winco Food Mart", "Yuan Ming Supermarket",
    "Pet Valu", "Ren's Pets", "PartSource",
    "Old Navy", "H&M", "Fabricland", "Chatters Salon", "Len's Mill Store",
}

def _deal_provinces(store):
    if store in _QC_ONLY_STORES:
        return ["QC"]
    if store in _ON_ONLY_STORES:
        return ["ON"]
    return ["QC", "ON"]


_STORE_CATEGORY = {
    # General
    "Amazon": "General",
    "Walmart": "General", "Costco": "General", "Giant Tiger": "General",
    "Rossy": "General", "Hart Stores": "General", "Dollarama": "General",
    # Grocery
    "Metro": "Grocery", "IGA": "Grocery", "Maxi": "Grocery", "Super C": "Grocery",
    "Provigo": "Grocery", "Adonis": "Grocery", "T&T Supermarket": "Grocery",
    "Wholesale Club and Club Entrepôt": "Grocery", "Rachelle Béry": "Grocery",
    "PA Nature": "Grocery", "Avril Supermarché Santé": "Grocery",
    "M&M Food Market": "Grocery", "Mayrand": "Grocery",
    "Marché Richelieu": "Grocery", "Les Marchés Tradition": "Grocery",
    "Real Canadian Superstore": "Grocery", "Your Independent Grocer": "Grocery",
    "No Frills": "Grocery", "Sobeys": "Grocery", "Food Basics": "Grocery",
    "FreshCo": "Grocery", "Loblaws": "Grocery", "Fortinos": "Grocery",
    "Farm Boy": "Grocery", "Longos": "Grocery", "Foodland": "Grocery",
    "Valu-Mart": "Grocery", "Independent City Market": "Grocery",
    "H Mart": "Grocery", "Highland Farms": "Grocery",
    "Btrust Supermarket": "Grocery", "Nations Fresh Foods": "Grocery",
    "Chalo FreshCo": "Grocery", "Fiesta Farms": "Grocery",
    "Lady York Foods": "Grocery", "Oceans Fresh Food Market": "Grocery",
    "Galleria Supermarket": "Grocery", "Foody Mart": "Grocery",
    "Foody World": "Grocery", "Seasons Food Mart": "Grocery",
    "Cataldi": "Grocery", "Terra Foodmart": "Grocery",
    "Fresh Land Supermarket": "Grocery", "Fusion Supermarket": "Grocery",
    "Starsky": "Grocery", "A1 Cash + Carry": "Grocery",
    "Bestco Foodmart": "Grocery", "Blue Sky Supermarket": "Grocery",
    "Danforth Food Market": "Grocery", "Pat Mart": "Grocery",
    "Tone Tai Supermarket": "Grocery", "Winco Food Mart": "Grocery",
    "Yuan Ming Supermarket": "Grocery",
    # Pharmacy
    "Jean Coutu": "Pharmacy", "Pharmaprix": "Pharmacy", "Familiprix": "Pharmacy",
    "Brunet": "Pharmacy", "Shoppers Drug Mart": "Pharmacy", "Uniprix": "Pharmacy",
    "Proxim": "Pharmacy", "Rexall": "Pharmacy", "Guardian": "Pharmacy",
    "Pharmasave": "Pharmacy", "Accès pharma": "Pharmacy", "Proximed": "Pharmacy",
    "Uniprix Sante": "Pharmacy", "Ki Nature & Santé": "Pharmacy",
    "I.D.A.": "Pharmacy",
    # Electronics
    "Best Buy": "Electronics", "EB Games Canada": "Electronics",
    "Staples": "Electronics", "Bureau en gros": "Electronics",
    "Centre Hi-Fi": "Electronics", "Tanguay": "Electronics",
    "Newegg": "Electronics", "2001 Audio Video": "Electronics",
    "The Source": "Electronics",
    # Home & Garden — all Rona/Lowe's brand variants
    "Home Depot": "Home & Garden", "RONA & RONA +": "Home & Garden",
    "Rona": "Home & Garden", "RONA": "Home & Garden", "Rona +": "Home & Garden",
    "Lowe's": "Home & Garden", "Lowes": "Home & Garden",
    "Home Hardware": "Home & Garden", "Canac": "Home & Garden",
    "Patrick Morin": "Home & Garden", "BMR": "Home & Garden",
    "Bath Depot": "Home & Garden", "Club Piscine": "Home & Garden",
    # Furniture
    "Canadian Tire": "General", "IKEA": "Furniture", "JYSK": "Furniture",
    "Leon's": "Furniture", "The Brick": "Furniture", "Linen Chest": "Furniture",
    "Stokes": "Furniture", "Dormez-Vous": "Furniture",
    "Wayfair": "Furniture", "Sleep Country Canada": "Furniture",
    "La-Z-Boy": "Furniture", "Oxford Mills": "Furniture",
    "Kitchen Stuff Plus": "Furniture",
    # Sporting & Outdoor
    "SAIL": "Sporting & Outdoor", "Sports Experts": "Sporting & Outdoor",
    "Sport Chek": "Sporting & Outdoor", "Atmosphere": "Sporting & Outdoor",
    "Decathlon": "Sporting & Outdoor", "Princess Auto": "Sporting & Outdoor",
    "Bumper to Bumper": "Sporting & Outdoor",
    "Cabela's & Bass Pro": "Sporting & Outdoor",
    # Clothing & Beauty
    "Old Navy": "Clothing", "H&M": "Clothing", "Fabricland": "Clothing",
    "Len's Mill Store": "Clothing", "Chatters Salon": "Clothing",
    "Sephora": "Clothing", "Holt Renfrew": "Clothing",
    "Reitmans": "Clothing", "Penningtons": "Clothing", "Laura": "Clothing",
    "Melanie Lyne": "Clothing", "Thyme Maternity": "Clothing",
    "Ardene": "Clothing", "Frank and Oak": "Clothing",
    "Addition Elle": "Clothing",
    # Pets
    "PetSmart": "Pets", "Mondou": "Pets", "Pet Valu": "Pets",
    "Ren's Pets": "Pets", "Animo Etc.": "Pets", "Chico": "Pets",
    # Arts & Crafts
    "Michaels Canada": "Arts & Crafts",
    "Long & McQuade Musical Instruments": "Arts & Crafts",
    # Specialty
    "SAQ": "Specialty", "LCBO": "Specialty", "BC Liquor Stores": "Specialty",
    "The Beer Store": "Specialty",
    "Party City": "Specialty", "Showcase": "Specialty",
    # Baby & Kids
    "Babies R Us": "Baby & Kids", "buybuy BABY": "Baby & Kids",
    "Carter's": "Baby & Kids", "OshKosh": "Baby & Kids",
    "Mastermind Toys": "Baby & Kids",
}


def _store_category(store):
    return _STORE_CATEGORY.get(store.strip(), "Other")


# ─────────────────────────────────────────────────────────
# SOURCE 4: Flipp.com  (Canadian flyers — grocery + retail)
# ─────────────────────────────────────────────────────────
_FLIPP_POSTAL_QC  = "H2Y1A1"   # Montreal
_FLIPP_POSTAL_ON  = "M5V3A8"   # Toronto
_FLIPP_GEN_PAGES  = 5           # 150 items/page for general search

# Merchants to fetch in full (entire flyer, not just general search results)
_FLIPP_TARGET_MERCHANTS = {
    # General
    "Walmart", "Costco", "Giant Tiger", "Rossy", "Hart Stores", "Dollarama",
    # Grocery — QC
    "Metro", "IGA", "Maxi", "Super C", "Provigo",
    "Adonis", "T&T Supermarket", "Wholesale Club and Club Entrepôt",
    "Rachelle Béry", "PA Nature", "Avril Supermarché Santé",
    "M&M Food Market", "Mayrand", "Marché Richelieu", "Les Marchés Tradition",
    # Grocery — ON
    "Real Canadian Superstore", "Your Independent Grocer",
    "No Frills", "Sobeys", "Food Basics", "FreshCo", "Loblaws",
    "Fortinos", "Farm Boy", "Longos", "Foodland", "Valu-Mart",
    "Independent City Market", "H Mart", "Highland Farms",
    "Btrust Supermarket", "Nations Fresh Foods", "Chalo FreshCo",
    # Pharmacy
    "Jean Coutu", "Pharmaprix", "Familiprix", "Brunet",
    "Shoppers Drug Mart", "Uniprix", "Proxim",
    "Rexall", "Guardian", "Pharmasave",
    "Accès pharma", "Proximed", "Uniprix Sante", "Ki Nature & Santé",
    # Electronics
    "Best Buy", "EB Games Canada", "Staples", "Bureau en gros",
    "Centre Hi-Fi", "Tanguay", "Newegg", "2001 Audio Video",
    # Home & Garden
    "Home Depot", "RONA & RONA +", "Rona", "RONA", "Rona +",
    "Lowe's", "Lowes", "Home Hardware", "Canac",
    "Patrick Morin", "BMR", "Bath Depot", "Club Piscine",
    # Furniture
    "Canadian Tire", "IKEA", "JYSK", "Leon's", "The Brick",
    "Linen Chest", "Stokes", "Dormez-Vous",
    "Wayfair", "Sleep Country Canada", "La-Z-Boy",
    # Sporting & Outdoor
    "SAIL", "Sports Experts", "Sport Chek", "Atmosphere", "Decathlon",
    "Princess Auto", "Bumper to Bumper", "Cabela's & Bass Pro",
    # Pets
    "PetSmart", "Mondou", "Pet Valu", "Ren's Pets",
    # Arts & Crafts
    "Michaels Canada", "Long & McQuade Musical Instruments", "Fabricland",
    # Clothing & Beauty
    "Old Navy", "H&M", "Chatters Salon", "Len's Mill Store", "Sephora",
    "Holt Renfrew", "Ardene", "Frank and Oak", "Addition Elle",
    # Specialty
    "SAQ", "LCBO", "The Beer Store",
}


def _flipp_parse_item(item, seen):
    iid = str(item.get("id") or item.get("flyer_item_id") or "")
    if not iid or iid in seen:
        return None
    seen.add(iid)

    title = _clean(item.get("name", ""))
    cur_p = item.get("current_price")
    if not title or cur_p is None:
        return None

    old_p      = item.get("original_price")
    store      = item.get("merchant_name", "") or "Flipp"
    img        = item.get("clean_image_url") or item.get("clipping_image_url") or ""
    valid_from = (item.get("valid_from") or "")[:10]
    valid_to   = (item.get("valid_to")   or "")[:10]
    sale_story = (item.get("sale_story")      or "").strip()
    pre        = (item.get("pre_price_text")  or "").strip()
    post       = (item.get("post_price_text") or "").strip()
    cat        = item.get("_L1") or ""
    flyer_id   = item.get("flyer_id", "")

    if sale_story:
        cur_str = sale_story
    elif pre or post:
        cur_str = f"{pre} ${cur_p:.2f} {post}".strip()
    else:
        cur_str = f"${cur_p:.2f}"

    old_str  = f"${old_p:.2f}" if isinstance(old_p, (int, float)) else ""
    save_pct = ""
    if isinstance(old_p, (int, float)) and old_p > cur_p:
        save_pct = str(round((old_p - cur_p) / old_p * 100)) + "%"

    return {
        "source":        "Flipp",
        "tid":           f"flipp-{iid}",
        "title":         title,
        "brand":         "",
        "store":         store,
        "link":          f"https://flipp.com/en-ca/flyer/{flyer_id}" if flyer_id else "",
        "currentPrice":  cur_str,
        "originalPrice": old_str,
        "savings":       "",
        "savePct":       save_pct,
        "pubDate":       valid_from,
        "relTime":       "Flyer",
        "votes":         0,
        "img":           img,
        "category":      cat,
        "clearance":     False,
        "dropDate":      valid_from,
        "validUntil":    valid_to,
        "provinces":     _deal_provinces(store),
    }


def get_flipp_deals():
    deals, errors = [], []
    seen = set()
    seen_lock = threading.Lock()

    # ── Step 1: fetch full flyers for target merchants (parallel) ──
    try:
        flyer_data = _get_json(
            f"https://backflipp.wishabi.com/flipp/flyers"
            f"?locale=en-ca&postal_code={_FLIPP_POSTAL_QC}&q=",
            referer="https://flipp.com/",
        )
        targets = []
        fetched_ids = set()
        for f in flyer_data.get("flyers", []):
            merchant = f.get("merchant") or ""
            fid      = f.get("id")
            if not fid or fid in fetched_ids:
                continue
            m_lower = merchant.lower()
            if not any(
                t.lower() in m_lower or
                (len(m_lower) >= 4 and m_lower in t.lower())
                for t in _FLIPP_TARGET_MERCHANTS
            ):
                continue
            fetched_ids.add(fid)
            targets.append((fid, merchant))

        def _fetch_flyer(fid_merchant):
            fid, merchant = fid_merchant
            url = (f"https://backflipp.wishabi.com/flipp/items/search"
                   f"?locale=en-ca&postal_code={_FLIPP_POSTAL_QC}"
                   f"&q=&flyer_ids={fid}")
            try:
                data = _get_json(url, referer="https://flipp.com/")
                return data.get("items", []), None
            except Exception as e:
                return [], f"Flipp {merchant}: {e}"

        with ThreadPoolExecutor(max_workers=8) as ex:
            for items, err in ex.map(_fetch_flyer, targets):
                if err:
                    errors.append(err)
                    continue
                with seen_lock:
                    for item in items:
                        d = _flipp_parse_item(item, seen)
                        if d:
                            deals.append(d)
    except Exception as e:
        errors.append(f"Flipp flyer list: {e}")

    # ── Step 2: general paginated search (QC + ON) for other stores ──
    for postal in (_FLIPP_POSTAL_QC, _FLIPP_POSTAL_ON):
        for page in range(1, _FLIPP_GEN_PAGES + 1):
            url = (f"https://backflipp.wishabi.com/flipp/items/search"
                   f"?locale=en-ca&postal_code={postal}&q=&page={page}")
            try:
                data  = _get_json(url, referer="https://flipp.com/")
                items = data.get("items", [])
                if not items:
                    break
                for item in items:
                    d = _flipp_parse_item(item, seen)
                    if d:
                        deals.append(d)
            except Exception as e:
                errors.append(f"Flipp {postal} p{page}: {e}")
                break

    return deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 6: Home Depot Canada (clearance + special buy)
# ─────────────────────────────────────────────────────────
def _hd_extract_product(item, out, seen, label):
    title = _clean(
        item.get("name") or item.get("title") or item.get("displayName") or
        item.get("productName") or ""
    )
    if not title:
        return

    pricing = item.get("pricing") or item.get("price") or {}
    if isinstance(pricing, dict):
        cur_val = (pricing.get("value") or pricing.get("current") or
                   pricing.get("sale") or pricing.get("salePrice") or 0)
        old_val = (pricing.get("original") or pricing.get("regular") or
                   pricing.get("regularPrice") or pricing.get("was") or 0)
    else:
        cur_val = item.get("salePrice") or item.get("currentPrice") or item.get("price") or 0
        old_val = item.get("originalPrice") or item.get("regularPrice") or 0

    try:
        cur = float(cur_val)
        old = float(old_val)
    except (TypeError, ValueError):
        cur, old = 0.0, 0.0

    if cur <= 0:
        return

    prod_id = str(
        item.get("id") or item.get("productId") or item.get("itemId") or
        item.get("sku") or item.get("omniPartNumber") or ""
    )
    if not prod_id:
        prod_id = hashlib.md5(title.encode()).hexdigest()[:10]

    tid = f"hd-direct-{prod_id}"
    if tid in seen:
        return
    seen.add(tid)

    url_path = (item.get("url") or item.get("link") or item.get("canonicalUrl") or
                item.get("productUrl") or "")
    if url_path and not url_path.startswith("http"):
        link = "https://www.homedepot.ca" + url_path
    else:
        link = url_path

    img = ""
    for k in ("images", "media", "mediaSet", "imageSet"):
        imgs = item.get(k)
        if isinstance(imgs, list) and imgs:
            first = imgs[0] if isinstance(imgs[0], dict) else {}
            img = first.get("src") or first.get("url") or first.get("thumbnailUrl") or ""
            if img:
                break
    if not img:
        for k in ("primaryImage", "image", "thumbnail", "imageUrl"):
            v = item.get(k)
            if isinstance(v, str) and v:
                img = v
                break

    save_pct = ""
    if old > cur > 0:
        save_pct = str(round((old - cur) / old * 100)) + "%"

    out.append({
        "source":        "HomeDepot",
        "tid":           tid,
        "title":         title,
        "brand":         _clean(item.get("brand") or item.get("brandName") or ""),
        "store":         "Home Depot",
        "link":          link,
        "currentPrice":  f"${cur:.2f}",
        "originalPrice": f"${old:.2f}" if old > cur else "",
        "savings":       f"Save ${old - cur:.2f}" if old > cur else "",
        "savePct":       save_pct,
        "pubDate":       "",
        "relTime":       label,
        "votes":         0,
        "img":           img,
        "category":      "",
        "clearance":     label == "Clearance",
        "dropDate":      "",
        "validUntil":    "",
        "provinces":     ["QC", "ON"],
    })


def _hd_walk(obj, out, seen, label, depth=0):
    """Walk parsed JSON looking for product lists, max depth 5."""
    if depth > 5 or len(out) >= 300:
        return
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and any(
            k in obj[0] for k in ("name", "title", "productId", "id", "omniPartNumber")
        ):
            for item in obj:
                if isinstance(item, dict):
                    _hd_extract_product(item, out, seen, label)
        else:
            for v in obj[:5]:
                _hd_walk(v, out, seen, label, depth + 1)
    elif isinstance(obj, dict):
        for k in ("products", "items", "results", "productList", "hits", "data"):
            v = obj.get(k)
            if isinstance(v, list) and v:
                before = len(out)
                for item in v:
                    if isinstance(item, dict):
                        _hd_extract_product(item, out, seen, label)
                if len(out) > before:
                    return
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                _hd_walk(v, out, seen, label, depth + 1)


def get_homedepot_deals():
    deals, errors = [], []
    seen = set()

    feeds = [
        ("Clearance",   "clearance"),
        ("Special Buy", "special+buy"),
    ]

    for label, q in feeds:
        for page in range(1, 5):
            sep = "&" if page > 1 else "?"
            url = (f"https://www.homedepot.ca/en/home/search.html?q={q}&pageSize=48"
                   + (f"&page={page}" if page > 1 else ""))
            try:
                text = _get(url)
                nd = re.search(
                    r'id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
                    text, re.S
                )
                if not nd:
                    errors.append(f"Home Depot {label}: no __NEXT_DATA__")
                    break
                data = json.loads(nd.group(1))
                before = len(deals)
                page_props = data.get("props", {}).get("pageProps", {})
                _hd_walk(page_props, deals, seen, label)
                if len(deals) == before:
                    break
            except Exception as e:
                errors.append(f"Home Depot {label} p{page}: {e}")
                break

    return deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 7: Rona Canada (clearance / promotions)
# ─────────────────────────────────────────────────────────
def _rona_extract_product(item, out, seen, label):
    title = _clean(
        item.get("name") or item.get("title") or item.get("displayName") or
        item.get("label") or ""
    )
    if not title:
        return

    # Rona uses "regularPrice" / "sellingPrice" or "price" sub-object
    pricing = item.get("price") or item.get("pricing") or {}
    if isinstance(pricing, dict):
        cur_val = (pricing.get("selling") or pricing.get("sale") or
                   pricing.get("current") or pricing.get("value") or 0)
        old_val = (pricing.get("regular") or pricing.get("original") or
                   pricing.get("was") or 0)
    else:
        cur_val = (item.get("sellingPrice") or item.get("salePrice") or
                   item.get("currentPrice") or item.get("price") or 0)
        old_val = item.get("regularPrice") or item.get("originalPrice") or 0

    try:
        cur = float(cur_val)
        old = float(old_val)
    except (TypeError, ValueError):
        cur, old = 0.0, 0.0

    if cur <= 0:
        return

    prod_id = str(
        item.get("id") or item.get("productId") or item.get("code") or
        item.get("sku") or item.get("articleNumber") or ""
    )
    if not prod_id:
        prod_id = hashlib.md5(title.encode()).hexdigest()[:10]

    tid = f"rona-direct-{prod_id}"
    if tid in seen:
        return
    seen.add(tid)

    url_path = (item.get("url") or item.get("link") or item.get("productUrl") or
                item.get("canonicalUrl") or "")
    if url_path and not url_path.startswith("http"):
        link = "https://www.rona.ca" + url_path
    else:
        link = url_path

    img = ""
    for k in ("images", "media", "pictures"):
        imgs = item.get(k)
        if isinstance(imgs, list) and imgs:
            first = imgs[0] if isinstance(imgs[0], dict) else {}
            img = first.get("url") or first.get("src") or ""
            if img:
                break
    if not img:
        for k in ("primaryImage", "image", "imageUrl", "thumbnail"):
            v = item.get(k)
            if isinstance(v, str) and v:
                img = v
                break
            elif isinstance(v, dict):
                img = v.get("url") or v.get("src") or ""
                if img:
                    break

    save_pct = ""
    if old > cur > 0:
        save_pct = str(round((old - cur) / old * 100)) + "%"

    out.append({
        "source":        "Rona",
        "tid":           tid,
        "title":         title,
        "brand":         _clean(item.get("brand") or item.get("brandName") or ""),
        "store":         "Rona",
        "link":          link,
        "currentPrice":  f"${cur:.2f}",
        "originalPrice": f"${old:.2f}" if old > cur else "",
        "savings":       f"Save ${old - cur:.2f}" if old > cur else "",
        "savePct":       save_pct,
        "pubDate":       "",
        "relTime":       label,
        "votes":         0,
        "img":           img,
        "category":      "",
        "clearance":     "clearance" in label.lower(),
        "dropDate":      "",
        "validUntil":    "",
        "provinces":     ["QC", "ON"],
    })


def _rona_walk(obj, out, seen, label, depth=0):
    if depth > 5 or len(out) >= 300:
        return
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and any(
            k in obj[0] for k in ("name", "title", "id", "productId", "code", "sku")
        ):
            for item in obj:
                if isinstance(item, dict):
                    _rona_extract_product(item, out, seen, label)
        else:
            for v in obj[:5]:
                _rona_walk(v, out, seen, label, depth + 1)
    elif isinstance(obj, dict):
        for k in ("products", "items", "results", "productList", "hits", "data", "articles"):
            v = obj.get(k)
            if isinstance(v, list) and v:
                before = len(out)
                for item in v:
                    if isinstance(item, dict):
                        _rona_extract_product(item, out, seen, label)
                if len(out) > before:
                    return
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                _rona_walk(v, out, seen, label, depth + 1)


def get_rona_deals():
    deals, errors = [], []
    seen = set()

    feeds = [
        ("Clearance",    "https://www.rona.ca/en/sale-clearance"),
        ("Promotions",   "https://www.rona.ca/en/promotions"),
        ("Special Buys", "https://www.rona.ca/en/special-buys"),
    ]

    for label, url in feeds:
        try:
            text = _get(url)
            nd = re.search(
                r'id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
                text, re.S
            )
            if not nd:
                # Try window.__PRELOADED_STATE__ fallback
                m = re.search(
                    r'window\.__(?:PRELOADED|INITIAL)_STATE__\s*=\s*(\{.*?\})\s*;',
                    text, re.S
                )
                if not m:
                    errors.append(f"Rona {label}: no JSON data found")
                    continue
                raw_json = m.group(1)
            else:
                raw_json = nd.group(1)

            data = json.loads(raw_json)
            before = len(deals)
            page_props = data.get("props", {}).get("pageProps", data)
            _rona_walk(page_props, deals, seen, label)
            if len(deals) == before:
                errors.append(f"Rona {label}: 0 products extracted")
        except Exception as e:
            errors.append(f"Rona {label}: {e}")

    return deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 8: LCBO (Ontario liquor board — sale items via Coveo search API)
# ─────────────────────────────────────────────────────────
_LCBO_COVEO_ORG   = "lcboproductionx2kwygnc"
_LCBO_COVEO_TOKEN = "xx883b5583-07fb-416b-874b-77cce565d927"


def _lcbo_get_token():
    """Extract fresh Coveo token from LCBO page if needed."""
    try:
        text = _get("https://www.lcbo.com/en/on-sale")
        m = re.search(r'configureCloudV2Endpoint\s*\(\s*["\']([^"\']+)["\'],\s*["\']([^"\']+)["\']', text)
        if m:
            return m.group(1), m.group(2)
    except Exception:
        pass
    return _LCBO_COVEO_ORG, _LCBO_COVEO_TOKEN


def get_lcbo_deals():
    deals, errors = [], []
    seen = set()
    org, token = _LCBO_COVEO_ORG, _LCBO_COVEO_TOKEN

    for first_result in range(0, 300, 100):
        body = json.dumps({
            "q": "",
            "numberOfResults": 100,
            "firstResult": first_result,
            "aq": "@ec_promo_price>0",
            "fieldsToInclude": [
                "ec_name", "ec_brand", "ec_price", "ec_promo_price",
                "ec_thumbnails", "ec_category", "permanentid",
            ],
        }).encode()
        req = urllib.request.Request(
            f"https://{org}.org.coveo.com/rest/search/v2",
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Origin": "https://www.lcbo.com",
                "Referer": "https://www.lcbo.com/",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
        except Exception as e:
            if "401" in str(e) and first_result == 0:
                # Token expired — re-fetch from page and retry once
                org, token = _lcbo_get_token()
                body = body  # rebuilt below on next iteration
                errors.append(f"LCBO: token refreshed")
                continue
            errors.append(f"LCBO p{first_result}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            raw = item.get("raw", {})
            title = _clean(raw.get("ec_name") or "")
            if not title:
                continue
            try:
                cur = float(raw.get("ec_promo_price") or 0)
                old = float(raw.get("ec_price") or 0)
            except (TypeError, ValueError):
                continue
            if cur <= 0:
                continue

            prod_id = str(raw.get("permanentid") or hashlib.md5(title.encode()).hexdigest()[:10])
            tid = f"lcbo-{prod_id}"
            if tid in seen:
                continue
            seen.add(tid)

            link = item.get("clickUri") or f"https://www.lcbo.com/en/search#{prod_id}"
            img = raw.get("ec_thumbnails") or ""
            if isinstance(img, list):
                img = img[0] if img else ""

            save_pct = ""
            if old > cur > 0:
                save_pct = str(round((old - cur) / old * 100)) + "%"

            deals.append({
                "source":        "LCBO",
                "tid":           tid,
                "title":         title,
                "brand":         _clean(str(raw.get("ec_brand") or "")),
                "store":         "LCBO",
                "link":          link,
                "currentPrice":  f"${cur:.2f}",
                "originalPrice": f"${old:.2f}" if old > cur else "",
                "savings":       f"Save ${old - cur:.2f}" if old > cur else "",
                "savePct":       save_pct,
                "pubDate":       "",
                "relTime":       "Sale",
                "votes":         0,
                "img":           img,
                "category":      "",
                "clearance":     False,
                "dropDate":      "",
                "validUntil":    "",
                "provinces":     ["ON"],
            })

    return deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 9: SAQ (Quebec liquor board — promotions)
# ─────────────────────────────────────────────────────────
def get_saq_deals():
    deals, errors = [], []
    seen = set()

    feeds = [
        ("Promotions", "https://www.saq.com/en/promotions"),
        ("Sale",       "https://www.saq.com/en/products/wine?features=on-sale&pageSize=96"),
        ("Clearance",  "https://www.saq.com/en/products?features=clearance&pageSize=96"),
    ]

    def _extract(item, label):
        title = _clean(
            item.get("name") or item.get("title") or item.get("displayName") or
            item.get("productName") or ""
        )
        if not title:
            return

        # SAQ uses priceBeforePromotion / price or price.value / compareAtPrice
        price_obj = item.get("price") or {}
        if isinstance(price_obj, dict):
            cur_val = price_obj.get("value") or price_obj.get("sale") or price_obj.get("current") or 0
            old_val = price_obj.get("original") or price_obj.get("regular") or 0
        else:
            cur_val = (item.get("currentPrice") or item.get("salePrice") or
                       price_obj or item.get("price") or 0)
            old_val = (item.get("priceBeforePromotion") or item.get("regularPrice") or
                       item.get("compareAtPrice") or 0)

        try:
            cur = float(cur_val)
            old = float(old_val)
        except (TypeError, ValueError):
            cur, old = 0.0, 0.0
        if cur <= 0:
            return

        prod_id = str(item.get("id") or item.get("code") or item.get("productId") or
                      item.get("sku") or "")
        if not prod_id:
            prod_id = hashlib.md5(title.encode()).hexdigest()[:10]
        tid = f"saq-{prod_id}"
        if tid in seen:
            return
        seen.add(tid)

        url_path = item.get("url") or item.get("link") or item.get("canonicalUrl") or ""
        if url_path and not url_path.startswith("http"):
            link = "https://www.saq.com" + url_path
        else:
            link = url_path

        img = ""
        for k in ("images", "media", "pictures"):
            imgs = item.get(k)
            if isinstance(imgs, list) and imgs:
                first = imgs[0] if isinstance(imgs[0], dict) else {}
                img = first.get("url") or first.get("src") or first.get("href") or ""
                if img:
                    break
        if not img:
            for k in ("image", "thumbnail", "imageUrl", "primaryImage"):
                v = item.get(k)
                if isinstance(v, str) and v:
                    img = v
                    break
                elif isinstance(v, dict):
                    img = v.get("url") or v.get("src") or ""
                    if img:
                        break

        save_pct = ""
        if old > cur > 0:
            save_pct = str(round((old - cur) / old * 100)) + "%"

        deals.append({
            "source":        "SAQ",
            "tid":           tid,
            "title":         title,
            "brand":         _clean(item.get("brand") or item.get("brandName") or ""),
            "store":         "SAQ",
            "link":          link,
            "currentPrice":  f"${cur:.2f}",
            "originalPrice": f"${old:.2f}" if old > cur else "",
            "savings":       f"Save ${old - cur:.2f}" if old > cur else "",
            "savePct":       save_pct,
            "pubDate":       "",
            "relTime":       "Promotion",
            "votes":         0,
            "img":           img,
            "category":      "",
            "clearance":     "clearance" in label.lower(),
            "dropDate":      "",
            "validUntil":    "",
            "provinces":     ["QC"],
        })

    def _walk(obj, label, depth=0):
        if depth > 5 or len(deals) >= 400:
            return
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict) and any(
                k in obj[0] for k in ("name", "title", "id", "code", "productId", "sku")
            ):
                for item in obj:
                    if isinstance(item, dict):
                        _extract(item, label)
            else:
                for v in obj[:5]:
                    _walk(v, label, depth + 1)
        elif isinstance(obj, dict):
            for k in ("products", "items", "hits", "results", "productList", "entries"):
                v = obj.get(k)
                if isinstance(v, list) and v:
                    before = len(deals)
                    for item in v:
                        if isinstance(item, dict):
                            _extract(item, label)
                    if len(deals) > before:
                        return
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    _walk(v, label, depth + 1)

    for label, url in feeds:
        try:
            text = _get(url)
            nd = re.search(r'id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', text, re.S)
            if not nd:
                # SAQ may use window.__PRELOADED_STATE__
                m = re.search(
                    r'window\.__(?:PRELOADED|INITIAL)_STATE__\s*=\s*(\{.*?\})\s*;',
                    text, re.S
                )
                if not m:
                    errors.append(f"SAQ {label}: no JSON data found")
                    continue
                raw = m.group(1)
            else:
                raw = nd.group(1)

            data = json.loads(raw)
            before = len(deals)
            page_props = data.get("props", {}).get("pageProps", data)
            _walk(page_props, label)
            if len(deals) == before:
                errors.append(f"SAQ {label}: 0 products extracted")
        except Exception as e:
            errors.append(f"SAQ {label}: {e}")

    return deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 10: BCLDB (BC liquor board — on-sale items)
# ─────────────────────────────────────────────────────────
def get_bcldb_deals():
    deals, errors = [], []
    seen = set()

    feeds = [
        ("Sale p1", "https://www.bcliquorstores.com/product-catalogue?promotion=on-sale&page=1"),
        ("Sale p2", "https://www.bcliquorstores.com/product-catalogue?promotion=on-sale&page=2"),
        ("Sale p3", "https://www.bcliquorstores.com/product-catalogue?promotion=on-sale&page=3"),
    ]

    def _extract(item, label):
        title = _clean(
            item.get("name") or item.get("title") or item.get("productName") or
            item.get("displayName") or ""
        )
        if not title:
            return

        price_obj = item.get("price") or item.get("pricing") or {}
        if isinstance(price_obj, dict):
            cur_val = (price_obj.get("current") or price_obj.get("sale") or
                       price_obj.get("promo") or price_obj.get("value") or 0)
            old_val = (price_obj.get("regular") or price_obj.get("list") or
                       price_obj.get("original") or 0)
        else:
            cur_val = (item.get("currentPrice") or item.get("salePrice") or
                       item.get("promotionPrice") or price_obj or 0)
            old_val = (item.get("regularPrice") or item.get("listPrice") or
                       item.get("originalPrice") or 0)

        try:
            cur = float(str(cur_val).replace("$", "").replace(",", "").strip())
            old = float(str(old_val).replace("$", "").replace(",", "").strip())
        except (TypeError, ValueError):
            cur, old = 0.0, 0.0
        if cur <= 0:
            return

        prod_id = str(item.get("id") or item.get("sku") or item.get("productId") or
                      item.get("code") or "")
        if not prod_id:
            prod_id = hashlib.md5(title.encode()).hexdigest()[:10]
        tid = f"bcldb-{prod_id}"
        if tid in seen:
            return
        seen.add(tid)

        url_path = item.get("url") or item.get("link") or item.get("canonicalUrl") or ""
        if url_path and not url_path.startswith("http"):
            link = "https://www.bcliquorstores.com" + url_path
        else:
            link = url_path or "https://www.bcliquorstores.com/product-catalogue?promotion=on-sale"

        img = ""
        for k in ("images", "media", "pictures", "galleryImages"):
            imgs = item.get(k)
            if isinstance(imgs, list) and imgs:
                first = imgs[0] if isinstance(imgs[0], dict) else {}
                img = first.get("url") or first.get("src") or first.get("href") or ""
                if img:
                    break
        if not img:
            for k in ("image", "thumbnail", "imageUrl", "primaryImage", "thumbnailImage"):
                v = item.get(k)
                if isinstance(v, str) and v:
                    img = v
                    break
                elif isinstance(v, dict):
                    img = v.get("url") or v.get("src") or ""
                    if img:
                        break

        save_pct = ""
        if old > cur > 0:
            save_pct = str(round((old - cur) / old * 100)) + "%"

        deals.append({
            "source":        "BCLDB",
            "tid":           tid,
            "title":         title,
            "brand":         _clean(item.get("brand") or item.get("brandName") or
                                    item.get("producer") or ""),
            "store":         "BC Liquor Stores",
            "link":          link,
            "currentPrice":  f"${cur:.2f}",
            "originalPrice": f"${old:.2f}" if old > cur else "",
            "savings":       f"Save ${old - cur:.2f}" if old > cur else "",
            "savePct":       save_pct,
            "pubDate":       "",
            "relTime":       "Sale",
            "votes":         0,
            "img":           img,
            "category":      _clean(item.get("category") or item.get("type") or
                                    item.get("productType") or ""),
            "clearance":     False,
            "dropDate":      "",
            "validUntil":    "",
            "provinces":     ["BC"],
        })

    def _walk(obj, label, depth=0):
        if depth > 5 or len(deals) >= 400:
            return
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict) and any(
                k in obj[0] for k in ("name", "title", "id", "productId", "sku", "code")
            ):
                for item in obj:
                    if isinstance(item, dict):
                        _extract(item, label)
            else:
                for v in obj[:5]:
                    _walk(v, label, depth + 1)
        elif isinstance(obj, dict):
            for k in ("products", "items", "hits", "results", "productList",
                      "entries", "catalogItems", "promotionItems"):
                v = obj.get(k)
                if isinstance(v, list) and v:
                    before = len(deals)
                    for item in v:
                        if isinstance(item, dict):
                            _extract(item, label)
                    if len(deals) > before:
                        return
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    _walk(v, label, depth + 1)

    for label, url in feeds:
        try:
            text = _get(url)
            nd = re.search(r'id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', text, re.S)
            if nd:
                data = json.loads(nd.group(1))
                before = len(deals)
                _walk(data.get("props", {}).get("pageProps", data), label)
                if len(deals) == before:
                    errors.append(f"BCLDB {label}: 0 products from __NEXT_DATA__")
                    break
                continue
            m = re.search(
                r'window\.__(?:PRELOADED|INITIAL)_STATE__\s*=\s*(\{.*?\})\s*;',
                text, re.S
            )
            if m:
                data = json.loads(m.group(1))
                before = len(deals)
                _walk(data, label)
                if len(deals) == before:
                    errors.append(f"BCLDB {label}: 0 products from __PRELOADED_STATE__")
                    break
                continue
            for block in re.findall(
                r'<script[^>]+type=["\']application/json["\'][^>]*>(.*?)</script>',
                text, re.S
            ):
                try:
                    before = len(deals)
                    _walk(json.loads(block), label)
                    if len(deals) > before:
                        break
                except Exception:
                    continue
            else:
                errors.append(f"BCLDB {label}: no JSON data found in page")
                break
        except Exception as e:
            errors.append(f"BCLDB {label}: {e}")
            break

    return deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 11: CamelCamelCamel RSS (Amazon Canada price drops)
# ─────────────────────────────────────────────────────────
_CAMEL_FEEDS = [
    ("https://camelcamelcamel.com/top_drops/ca/rss",             "General"),
    ("https://camelcamelcamel.com/top_drops/ca/electronics/rss", "Electronics"),
    ("https://camelcamelcamel.com/top_drops/ca/kitchen/rss",     "Home & Kitchen"),
    ("https://camelcamelcamel.com/top_drops/ca/sports/rss",      "Sporting & Outdoor"),
    ("https://camelcamelcamel.com/top_drops/ca/toys/rss",        "Baby & Kids"),
    ("https://camelcamelcamel.com/top_drops/ca/tools/rss",       "Home & Garden"),
    ("https://camelcamelcamel.com/top_drops/ca/clothing/rss",    "Clothing"),
]


def _camel_parse_feed(url, default_category):
    deals = []
    try:
        xml_text = _get(url, extra={"Accept": "application/rss+xml,application/xml,*/*",
                                     "Referer": "https://camelcamelcamel.com/"})
        root = ET.fromstring(xml_text)
    except Exception as e:
        return deals, f"CamelCamelCamel {url}: {e}"

    channel = root.find("channel")
    if channel is None:
        return deals, f"CamelCamelCamel {url}: no <channel>"

    seen_asins = set()
    for item in channel.findall("item"):
        title_raw = _clean((item.findtext("title") or "").strip())
        link_raw  = (item.findtext("link") or "").strip()
        desc_raw  = _clean(item.findtext("description") or "")
        pub_raw   = (item.findtext("pubDate") or "")

        if not title_raw or not link_raw:
            continue

        # ASIN from CCC URL e.g. /product/B07XXX
        asin_m = re.search(r'/product/([A-Z0-9]{10})', link_raw)
        if not asin_m:
            continue
        asin = asin_m.group(1)
        if asin in seen_asins:
            continue
        seen_asins.add(asin)

        cur_price = 0.0
        was_price = 0.0
        save_pct  = ""
        clean_title = title_raw

        # "Title — dropped to CA$X.XX from CA$Y.YY (Z% decrease)"
        m = re.search(
            r'\s*[—\-]\s*dropped\s+to\s+(?:CA)?\$?\s*([\d,.]+)'
            r'\s+from\s+(?:CA)?\$?\s*([\d,.]+)'
            r'(?:\s*\((\d+)%)?',
            title_raw, re.I
        )
        if m:
            try:
                cur_price = float(m.group(1).replace(',', ''))
                was_price = float(m.group(2).replace(',', ''))
                save_pct  = (m.group(3) + "%") if m.group(3) else ""
            except (ValueError, TypeError):
                pass
            clean_title = title_raw[:m.start()].strip()
        else:
            # Fallback: grab any prices from title or description
            prices = re.findall(r'(?:CA)?\$\s*([\d,]+\.\d{2})', title_raw + " " + desc_raw)
            if prices:
                try:
                    cur_price = float(prices[0].replace(',', ''))
                    if len(prices) > 1:
                        was_price = float(prices[1].replace(',', ''))
                except (ValueError, TypeError):
                    pass
            pct_m = re.search(r'(\d+)\s*%\s*(?:off|decrease|drop)', title_raw + " " + desc_raw, re.I)
            if pct_m:
                save_pct = pct_m.group(1) + "%"

        if not save_pct and was_price > cur_price > 0:
            save_pct = str(round((was_price - cur_price) / was_price * 100)) + "%"

        # Parse pubDate to YYYY-MM-DD
        pub_date = ""
        try:
            import email.utils
            pub_date = datetime(*email.utils.parsedate(pub_raw)[:3]).strftime("%Y-%m-%d")
        except Exception:
            pub_date = pub_raw[:10] if pub_raw else ""

        cur_str = f"${cur_price:.2f}" if cur_price > 0 else ""
        was_str = f"${was_price:.2f}" if was_price > cur_price > 0 else ""
        sav_str = f"Save ${was_price - cur_price:.2f}" if was_price > cur_price > 0 else ""

        deals.append({
            "source":        "CamelCamelCamel",
            "tid":           f"camel-{asin}",
            "title":         clean_title or title_raw,
            "brand":         "",
            "store":         "Amazon",
            "link":          f"https://www.amazon.ca/dp/{asin}/",
            "currentPrice":  cur_str,
            "originalPrice": was_str,
            "savings":       sav_str,
            "savePct":       save_pct,
            "pubDate":       pub_date,
            "relTime":       "Price Drop",
            "votes":         0,
            "img":           f"https://images-na.ssl-images-amazon.com/images/P/{asin}.01._SX300_.jpg",
            "category":      default_category,
            "clearance":     False,
            "dropDate":      pub_date,
            "validUntil":    "",
            "provinces":     ["QC", "ON"],
        })

    return deals, None


def get_camel_deals():
    all_deals, errors = [], []
    seen = set()

    def _fetch(feed_info):
        url, category = feed_info
        return _camel_parse_feed(url, category)

    with ThreadPoolExecutor(max_workers=3) as ex:
        for deals, err in ex.map(_fetch, _CAMEL_FEEDS):
            if err:
                errors.append(err)
            for d in deals:
                if d["tid"] not in seen:
                    seen.add(d["tid"])
                    all_deals.append(d)

    return all_deals, errors


_SOURCES = [
    ("walmart",      get_walmart_deals,      "Walmart"),
    ("stocktrack",   get_stocktrack_deals,   "StockTrack"),
    ("redflagdeals", get_redflagdeals,       "RedFlagDeals"),
    ("flipp",        get_flipp_deals,        "Flipp"),
    ("shopify",      get_shopify_deals,      "Shopify"),
    ("homedepot",    get_homedepot_deals,    "HomeDepot"),
    ("rona",         get_rona_deals,         "Rona"),
    ("lcbo",         get_lcbo_deals,         "LCBO"),
    ("saq",          get_saq_deals,          "SAQ"),
    ("bcldb",        get_bcldb_deals,        "BC Liquor Stores"),
    ("camel",        get_camel_deals,        "Amazon"),
]


def _background_refresh():
    last_fp = None
    while True:
        shared = {key: ([], []) for key, _, _ in _SOURCES}
        shared_lock = threading.Lock()

        def _worker(key, fetch_fn, label):
            try:
                deals, errs = fetch_fn()
            except Exception as ex:
                deals, errs = [], [str(ex)]
            with shared_lock:
                shared[key] = (deals, errs)
            # Log immediately but do NOT update the public cache yet —
            # we only commit once all sources are done so the API never
            # returns a partial result (which caused stores to flicker away).
            print(f"[{datetime.now():%H:%M:%S}] {label} loaded: {len(deals)} deals")

        threads = [
            threading.Thread(target=_worker, args=(k, fn, lbl), daemon=True)
            for k, fn, lbl in _SOURCES
        ]
        for t in threads:
            t.start()
        # Wait up to 90 s per thread — a hung source can't block the full cycle
        for t in threads:
            t.join(timeout=90)

        # All sources done (or timed out) — build and publish atomically
        result = _build_and_cache(dict(shared))

        if result:
            fp = _fingerprint(result)
            if last_fp is None:
                print(f"[{datetime.now():%H:%M:%S}] Initial load complete: "
                      f"{result['count']} deals "
                      f"(Walmart={result['walmart']}, "
                      f"StockTrack={result['stocktrack']}, "
                      f"RedFlagDeals={result['redflagdeals']})")
            elif fp != last_fp:
                payload = json.dumps({
                    "type":    "update",
                    "count":   result["count"],
                    "fetched": result["fetched"],
                })
                _push_sse(payload)
                print(f"[{datetime.now():%H:%M:%S}] Deals updated "
                      f"({result['count']} total) — SSE pushed")
            last_fp = fp
            _check_alerts(result["deals"])

        time.sleep(600)   # 10 minutes


# ─────────────────────────────────────────────────────────
# HTTP handler
# ─────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{datetime.now():%H:%M:%S}] {fmt % args}")

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        ae = self.headers.get("Accept-Encoding", "")
        if "gzip" in ae:
            body = gzip.compress(body, compresslevel=6)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
        else:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, fpath, ctype):
        try:
            with open(fpath, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/api/deals":
            if not _rate_ok(self.client_address[0]):
                self._send_json({"error": "Too many requests"}, 429)
                return
            with _cache_lock:
                result = _cached_result
            if result is None:
                self._send_json({"error": "Loading…", "deals": []}, 503)
            else:
                public = {k: v for k, v in result.items() if k != "errors"}
                self._send_json(public)

        elif path == "/api/status":
            with _cache_lock:
                result = _cached_result
            if result is None:
                self._send_json({"ready": False})
            else:
                self._send_json({
                    "ready":       True,
                    "count":       result["count"],
                    "walmart":     result["walmart"],
                    "stocktrack":  result["stocktrack"],
                    "storeCounts": result["storeCounts"],
                    "fetched":     result["fetched"],
                })

        elif path == "/api/watchdog":
            import subprocess as _sp
            _pid = os.getpid()
            _uptime_s = -1
            try:
                with open(f"/proc/{_pid}/stat") as _f:
                    _start_ticks = int(_f.read().split()[21])
                with open("/proc/uptime") as _f:
                    _sys_up = float(_f.read().split()[0])
                _uptime_s = int(_sys_up - _start_ticks / os.sysconf(os.sysconf_names["SC_CLK_TCK"]))
            except Exception:
                pass

            with _cache_lock:
                _res = _cached_result
            if _res:
                _fetched = _res.get("fetched", "")
                try:
                    _ts = datetime.fromisoformat(_fetched.replace("Z", "+00:00")).timestamp()
                    _age = int(time.time() - _ts)
                except Exception:
                    _age = -1
                _deals = {"count": _res.get("count", 0), "fetched": _fetched,
                          "age_s": _age, "stale": _age > 900}
            else:
                _deals = {"count": 0, "fetched": None, "age_s": -1, "stale": True}

            _tunnel_url = ""
            try:
                _idx_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
                with open(_idx_path) as _f:
                    _m = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com", _f.read())
                if _m:
                    _tunnel_url = _m.group(0)
            except Exception:
                pass

            _cf_running, _cf_pid = False, None
            try:
                _pr = _sp.run(["pgrep", "-f", "cloudflared.*tunnel"],
                              capture_output=True, text=True, timeout=3)
                _cf_pids = [int(p) for p in _pr.stdout.split() if p.strip()]
                _cf_running = bool(_cf_pids)
                _cf_pid = _cf_pids[0] if _cf_pids else None
            except Exception:
                pass

            _log_lines, _log_mtime = [], None
            try:
                _lp = "/tmp/watchdog.log"
                if os.path.exists(_lp):
                    _log_mtime = os.path.getmtime(_lp)
                    with open(_lp) as _f:
                        _log_lines = [l.rstrip() for l in _f.readlines()[-50:]]
            except Exception:
                pass

            self._send_json({
                "server":   {"healthy": True, "pid": _pid, "uptime_s": _uptime_s},
                "deals":    _deals,
                "tunnel":   {"url": _tunnel_url, "cf_running": _cf_running, "cf_pid": _cf_pid},
                "watchdog": {"last_run_ts": _log_mtime, "log": _log_lines},
            })

        elif path == "/api/health-report":
            with _reports_lock:
                self._send_json(_health_report or {"status": "no_report_yet"})

        elif path == "/api/scout-report":
            with _reports_lock:
                self._send_json(_scout_report or {"status": "no_report_yet"})

        elif path == "/api/command":
            with _reports_lock:
                self._send_json(_pending_command)

        elif path == "/api/board":
            with _reports_lock:
                self._send_json({"messages": list(_team_board)[-30:]})

        elif path == "/api/clicks":
            with _click_lock:
                self._send_json(dict(_click_counts))

        elif path == "/api/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            q = []
            with _sse_lock:
                _sse_clients.append(q)

            try:
                self.wfile.write(b": ping\n\n")
                self.wfile.flush()
            except Exception:
                pass

            try:
                while True:
                    if q:
                        payload = q.pop(0)
                        self.wfile.write(f"data: {payload}\n\n".encode())
                        self.wfile.flush()
                    else:
                        try:
                            self.wfile.write(b": heartbeat\n\n")
                            self.wfile.flush()
                        except Exception:
                            break
                        time.sleep(30)
            except Exception:
                pass
            finally:
                with _sse_lock:
                    if q in _sse_clients:
                        _sse_clients.remove(q)

        elif path == "/api/history":
            qs = {}
            if "?" in self.path:
                for kv in self.path.split("?", 1)[1].split("&"):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        qs[urllib.parse.unquote_plus(k)] = urllib.parse.unquote_plus(v)
            tid = qs.get("tid", "")
            if not tid:
                self._send_json({"error": "missing tid"}, 400)
            else:
                try:
                    with _db_lock:
                        with sqlite3.connect(DB_PATH, timeout=10) as conn:
                            rows = conn.execute(
                                "SELECT price, seen_at FROM price_points "
                                "WHERE tid=? ORDER BY seen_at",
                                (tid,)
                            ).fetchall()
                    self._send_json({"points": [{"price": r[0], "at": r[1]} for r in rows]})
                except Exception as ex:
                    self._send_json({"error": str(ex)}, 500)

        elif path in ("/monitor", "/monitor.html"):
            self._serve_file("monitor.html", "text/html; charset=utf-8")

        elif path in ("/", "/index.html"):
            self._serve_file("app.html", "text/html; charset=utf-8")

        elif path in ("/app.html", "/quebec_ontario_deals.html"):
            self._serve_file("app.html", "text/html; charset=utf-8")

        elif path == "/manifest.json":
            self._serve_file("manifest.json", "application/manifest+json")

        elif path == "/service-worker.js":
            self._serve_file("service-worker.js", "application/javascript")

        elif path in ("/icon-192.png", "/icon-512.png"):
            self._serve_file(path.lstrip("/"), "image/png")

        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_POST(self):
        path = self.path.split("?")[0]

        if path == "/api/login":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                if body.get("password") == _monitor_password:
                    self._send_json({"ok": True, "token": _admin_token})
                else:
                    self._send_json({"ok": False, "error": "Wrong password"}, 401)
            except Exception as ex:
                self._send_json({"error": str(ex)}, 400)
            return

        if path == "/api/command":
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {_admin_token}":
                self._send_json({"error": "Unauthorized"}, 401)
                return
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                with _reports_lock:
                    _pending_command.clear()
                    _pending_command.update({
                        "status": "pending",
                        "text": body.get("text", ""),
                        "issued_at": datetime.utcnow().isoformat() + "Z",
                    })
                self._send_json({"ok": True})
            except Exception as ex:
                self._send_json({"error": str(ex)}, 400)
            return

        if path == "/api/command/done":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                with _reports_lock:
                    _pending_command.update({
                        "status": "done",
                        "result": body.get("result", ""),
                        "done_at": datetime.utcnow().isoformat() + "Z",
                    })
                self._send_json({"ok": True})
            except Exception as ex:
                self._send_json({"error": str(ex)}, 400)
            return

        if path == "/api/click":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                tid = str(body.get("tid", ""))[:128]
                if tid:
                    with _click_lock:
                        _click_counts[tid] = _click_counts.get(tid, 0) + 1
                self._send_json({"ok": True})
            except Exception as ex:
                self._send_json({"error": str(ex)}, 400)
            return

        if path == "/api/board":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                entry = {
                    "from": str(body.get("from", ""))[:32],
                    "to":   str(body.get("to",   ""))[:32],
                    "type": str(body.get("type", ""))[:32],
                    "text": str(body.get("text", ""))[:500],
                    "ts":   datetime.utcnow().isoformat() + "Z",
                }
                with _reports_lock:
                    _team_board.append(entry)
                    if len(_team_board) > 100:
                        _team_board[:] = _team_board[-100:]
                self._send_json({"ok": True})
            except Exception as ex:
                self._send_json({"error": str(ex)}, 400)
            return

        if path in ("/api/health-report", "/api/scout-report"):
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                body["received_at"] = datetime.utcnow().isoformat() + "Z"
                with _reports_lock:
                    if path == "/api/health-report":
                        _health_report.clear()
                        _health_report.update(body)
                    else:
                        _scout_report.clear()
                        _scout_report.update(body)
                self._send_json({"ok": True})
            except Exception as ex:
                self._send_json({"error": str(ex)}, 400)

        elif path == "/api/alerts":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                email = body.get("email", "").strip()
                tid   = body.get("tid", "").strip()
                target = float(body.get("target_price", 0))
                title  = body.get("title", "")[:120]
                store  = body.get("store", "")[:80]
                link   = body.get("link", "")[:500]
                if not email or not tid or target <= 0:
                    self._send_json({"error": "invalid fields"}, 400)
                    return
                now = datetime.utcnow().isoformat()
                with _db_lock:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        conn.execute(
                            "INSERT INTO alerts(email,tid,store,title,target_price,link,created_at) "
                            "VALUES(?,?,?,?,?,?,?)",
                            (email, tid, store, title, target, link, now)
                        )
                        conn.commit()
                self._send_json({"ok": True})
            except Exception as ex:
                self._send_json({"error": str(ex)}, 500)
        else:
            self.send_response(404)
            self.end_headers()


# ─────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    t = threading.Thread(target=_background_refresh, daemon=True)
    t.start()

    server = ThreadedHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[{datetime.now():%H:%M:%S}] Server at http://0.0.0.0:{PORT}")
    print(f"  Windows: http://192.168.1.204:{PORT}/")
    print(f"  Sources : Walmart.ca (flash deals + rollbacks + clearance)")
    print(f"            StockTrack.ca (Staples, Home Depot, Rona, Best Buy…)")
    print(f"  Fetching initial deals…")
    server.serve_forever()
