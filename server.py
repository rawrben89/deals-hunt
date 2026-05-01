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
import urllib.request
import gzip
import re
import html as html_mod
import threading
import time
import hashlib
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from datetime import datetime

PORT = 8080

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
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
    try:
        return gzip.decompress(raw).decode("utf-8", errors="replace")
    except Exception:
        return raw.decode("utf-8", errors="replace")


def _get_json(url, referer="https://stocktrack.ca/"):
    h = dict(_JSON_H)
    h["Referer"] = referer
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=15) as r:
        raw = r.read()
    try:
        raw = gzip.decompress(raw)
    except Exception:
        pass
    return json.loads(raw.decode("utf-8", errors="replace"))


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
    ("1984137688763", "Flash Deals",        "special_offers%3AReduced+Price", 6),
    ("6000205319047", "Rollbacks",          "",                               5),
    ("6000197551406", "Clearance",          "",                               4),
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

            # For accuracy: require was_price for Walmart flash deals
            # (rollbacks may not have it — still include but mark clearly)
            save_pct = _save_pct(cur_price, was_price)

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
    all_deals, errors, seen = [], [], set()

    for shelf_id, label, facet, max_pages in WALMART_FEEDS:
        base = f"https://www.walmart.ca/en/shop/{shelf_id.lower()}/{shelf_id}"
        if facet:
            base += f"?facet={facet}"

        for page in range(1, max_pages + 1):
            sep = "&" if facet else "?"
            url = base + (f"{sep}page={page}" if page > 1 else "")
            try:
                page_deals = _parse_walmart_page(url)
                added = 0
                for d in page_deals:
                    key = d["tid"]
                    if key and key not in seen:
                        seen.add(key)
                        d["relTime"] = label
                        d["clearance"] = (label == "Clearance")
                        all_deals.append(d)
                        added += 1
                if not page_deals or added == 0:
                    break           # no new items on this page
                time.sleep(0.4)    # be polite
            except Exception as e:
                errors.append(f"Walmart {label} p{page}: {e}")
                break

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
    all_deals, errors = [], []
    for sid, (sname, url_tmpl) in ST_STORES.items():
        url = (f"{ST_BASE}/{sid}/drops_data.php"
               "?t=all&sort=date&dir=desc&oos=false")
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

                all_deals.append({
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
            errors.append(f"StockTrack {sid}: {e}")
    return all_deals, errors


# ─────────────────────────────────────────────────────────
# SOURCE 3: RedFlagDeals.com  (hot deals RSS)
# ─────────────────────────────────────────────────────────
_RFD_STORES = [
    "Amazon", "Walmart", "Costco", "Best Buy", "Canadian Tire",
    "Staples", "Home Depot", "IKEA", "Sport Chek", "Lowe's", "Lowes",
    "Rona", "Winners", "H&M", "Old Navy", "Gap", "The Source",
    "Shoppers Drug Mart", "Shoppers", "London Drugs", "Sobeys",
    "Metro", "Princess Auto", "Decathlon", "Atmosphere", "Reitmans",
    "La Maison Simons", "Simons", "Bureau en Gros", "SportiumCOUNT",
]


def _rfd_store(title):
    tl = title.lower()
    for s in _RFD_STORES:
        if s.lower() in tl:
            return s
    return "RedFlagDeals"


def get_redflagdeals():
    deals, errors = [], []
    try:
        text = _get("https://forums.redflagdeals.com/feed/forum/9/")
        root = ET.fromstring(text)
        channel = root.find("channel")
        items = (channel.findall("item") if channel is not None
                 else root.findall(".//item"))

        for item in items[:80]:
            title = _clean(item.findtext("title", ""))
            link  = (item.findtext("link", "") or "").strip()
            desc  = _clean(item.findtext("description", ""))
            pub   = (item.findtext("pubDate", "") or "").strip()

            if not title:
                continue

            # Extract price from title or description
            pm = re.search(r'\$\s*(\d[\d,]*(?:\.\d{1,2})?)', title + " " + desc)
            price = f"${pm.group(1)}" if pm else ""

            # Detect discount %
            pct_m = re.search(r'(\d+)\s*%\s*off', title + " " + desc, re.I)
            save_pct = f"{pct_m.group(1)}%" if pct_m else ""

            store = _rfd_store(title)
            tid   = "rfd-" + hashlib.md5(link.encode()).hexdigest()[:10]

            # Parse RFC-2822 pubDate → YYYY-MM-DD
            drop_date = ""
            if pub:
                try:
                    from email.utils import parsedate
                    t = parsedate(pub)
                    if t:
                        drop_date = f"{t[0]:04d}-{t[1]:02d}-{t[2]:02d}"
                except Exception:
                    pass

            is_clearance = bool(re.search(r'\bclearance\b', title + " " + desc, re.I))

            deals.append({
                "source":        "RedFlagDeals",
                "tid":           tid,
                "title":         title,
                "brand":         "",
                "store":         store,
                "link":          link,
                "currentPrice":  price,
                "originalPrice": "",
                "savings":       "",
                "savePct":       save_pct,
                "pubDate":       pub,
                "relTime":       "Hot Deal",
                "votes":         0,
                "img":           "",
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
# SOURCE 5: Shopify clothing retailers (sale collections)
# ─────────────────────────────────────────────────────────
_SHOPIFY_CLOTHING_STORES = [
    ("Reitmans",        "www.reitmans.com"),
    ("Penningtons",     "penningtons.com"),
    ("Laura",           "www.laura.ca"),
    ("Melanie Lyne",    "www.melanielyne.com"),
    ("Thyme Maternity", "thymematernity.com"),
]


def get_shopify_clothing():
    deals, errors = [], []
    seen = set()

    for store_name, domain in _SHOPIFY_CLOTHING_STORES:
        for page in range(1, 4):
            url = (f"https://{domain}/collections/sale/products.json"
                   f"?limit=250&page={page}")
            try:
                data = _get_json(url, referer=f"https://{domain}/")
                products = data.get("products", [])
                if not products:
                    break
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

                        tid = f"shopcloth-{domain}-{handle}-{v.get('id','')}"
                        if tid in seen:
                            continue
                        seen.add(tid)

                        save_amt = round(old - cur, 2)
                        save_pct = str(round((old - cur) / old * 100)) + "%"
                        link     = f"https://{domain}/products/{handle}"

                        deals.append({
                            "source":        "Shopify",
                            "tid":           tid,
                            "title":         title,
                            "brand":         brand,
                            "store":         store_name,
                            "link":          link,
                            "currentPrice":  f"${cur:.2f}",
                            "originalPrice": f"${old:.2f}",
                            "savings":       f"Save ${save_amt:.2f}",
                            "savePct":       save_pct,
                            "pubDate":       "",
                            "relTime":       "Sale",
                            "votes":         0,
                            "img":           img,
                            "category":      "Clothing",
                            "clearance":     False,
                            "dropDate":      "",
                            "validUntil":    "",
                            "provinces":     _deal_provinces(store_name),
                        })
                        break  # one variant per product is enough
            except Exception as e:
                errors.append(f"Shopify {store_name} p{page}: {e}")
                break

    return deals, errors


# ─────────────────────────────────────────────────────────
# Combined fetch + background cache
# ─────────────────────────────────────────────────────────
_cache_lock    = threading.Lock()
_cached_result = None
_sse_clients   = []
_sse_lock      = threading.Lock()


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

    store_counts = {}
    for d in all_deals:
        store_counts[d["store"]] = store_counts.get(d["store"], 0) + 1

    result = {
        "deals":        all_deals,
        "count":        len(all_deals),
        "walmart":      counts.get("walmart", 0),
        "stocktrack":   counts.get("stocktrack", 0),
        "redflagdeals": counts.get("redflagdeals", 0),
        "flipp":        counts.get("flipp", 0),
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
    "Home Depot", "RONA & RONA +", "Home Hardware", "Canac",
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
    "Holt Renfrew",
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

    # ── Step 1: fetch full flyers for target merchants ──
    try:
        flyer_data = _get_json(
            f"https://backflipp.wishabi.com/flipp/flyers"
            f"?locale=en-ca&postal_code={_FLIPP_POSTAL_QC}&q=",
            referer="https://flipp.com/",
        )
        fetched_ids = set()
        for f in flyer_data.get("flyers", []):
            merchant = f.get("merchant", "")
            fid      = f.get("id")
            if not fid or fid in fetched_ids:
                continue
            if not any(t.lower() in merchant.lower()
                       for t in _FLIPP_TARGET_MERCHANTS):
                continue
            fetched_ids.add(fid)
            url = (f"https://backflipp.wishabi.com/flipp/items/search"
                   f"?locale=en-ca&postal_code={_FLIPP_POSTAL_QC}"
                   f"&q=&flyer_ids={fid}")
            try:
                data  = _get_json(url, referer="https://flipp.com/")
                for item in data.get("items", []):
                    d = _flipp_parse_item(item, seen)
                    if d:
                        deals.append(d)
                time.sleep(0.25)
            except Exception as e:
                errors.append(f"Flipp {merchant}: {e}")
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


_SOURCES = [
    ("walmart",      get_walmart_deals,      "Walmart"),
    ("stocktrack",   get_stocktrack_deals,   "StockTrack"),
    ("redflagdeals", get_redflagdeals,       "RedFlagDeals"),
    ("flipp",        get_flipp_deals,        "Flipp"),
    ("shopcloth",    get_shopify_clothing,   "ShopCloth"),
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
                snap = dict(shared)
            result = _build_and_cache(snap)
            print(f"[{datetime.now():%H:%M:%S}] {label} loaded: "
                  f"{len(deals)} deals (total cached: {result['count']})")

        threads = [
            threading.Thread(target=_worker, args=(k, fn, lbl), daemon=True)
            for k, fn, lbl in _SOURCES
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with _cache_lock:
            result = _cached_result

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

        time.sleep(300)   # 5 minutes


# ─────────────────────────────────────────────────────────
# HTTP handler
# ─────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{datetime.now():%H:%M:%S}] {fmt % args}")

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
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
            with _cache_lock:
                result = _cached_result
            if result is None:
                self._send_json({"error": "Loading…", "deals": []}, 503)
            else:
                self._send_json(result)

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

        elif path in ("/", "/index.html", "/quebec_ontario_deals.html"):
            self._serve_file("quebec_ontario_deals.html", "text/html; charset=utf-8")

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
