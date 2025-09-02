#!/usr/bin/env python3
"""
apple_refurbs_playwright.py

Playwright crawler for Apple refurbished storefronts.
- BFS over categories (mac/ipad/iphone/watch/accessories...)
- Strict product-url heuristics so source_url is a concrete product page
- Verbose prints parsed products live

Usage:
  python apple_refurbs_playwright.py --countries FR,DE --verbose --max-per-country 300 --max-pages 200
"""

import asyncio
import argparse
import json
import re
import random
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from html import unescape
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from asyncio import Semaphore

# ---------- Defaults ----------
DEFAULT_COUNTRY_START_URLS = {
    "US": "https://www.apple.com/shop/refurbished",
    "CA": "https://www.apple.com/ca/shop/refurbished",
    "MX": "https://www.apple.com/mx/shop/refurbished",
    "GB": "https://www.apple.com/uk/shop/refurbished",
    "DE": "https://www.apple.com/de/shop/refurbished",
    "FR": "https://www.apple.com/fr/shop/refurbished",
    "ES": "https://www.apple.com/es/shop/refurbished",
    "IT": "https://www.apple.com/it/shop/refurbished",
    "NL": "https://www.apple.com/nl/shop/refurbished",
    "SE": "https://www.apple.com/se/shop/refurbished",
}

RANDOM_DELAY = (0.2, 1.0)
PAGE_TIMEOUT = 30000
PRODUCT_PAGE_TIMEOUT = 30000
CONCURRENT_PRODUCT_FETCHES = 6

DEFAULT_MAX_PAGES_PER_COUNTRY = 200
DEFAULT_MAX_PRODUCTS_PER_COUNTRY = 1000
MAX_BFS_DEPTH = 3


# ---------- Parsing helpers ----------
def extract_ld_json(html):
    blocks = []
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                         html, flags=re.DOTALL | re.IGNORECASE):
        content = unescape(m.group(1).strip())
        try:
            obj = json.loads(content)
            blocks.append(obj)
            continue
        except Exception:
            candidates = re.findall(r'(\{(?:[^{}]|(?1))*\})', content)
            for c in candidates:
                try:
                    blocks.append(json.loads(c))
                except Exception:
                    pass
    return blocks


def meta_tag(html, name=None, prop=None):
    if prop:
        m = re.search(r'<meta[^>]+property=["\']%s["\'][^>]*content=["\']([^"\']*)' % re.escape(prop),
                      html, flags=re.IGNORECASE)
        if m:
            return unescape(m.group(1).strip())
    if name:
        m = re.search(r'<meta[^>]+name=["\']%s["\'][^>]*content=["\']([^"\']*)' % re.escape(name),
                      html, flags=re.IGNORECASE)
        if m:
            return unescape(m.group(1).strip())
    return None


def find_first_image_from_imagegallery(blocks):
    for b in blocks:
        if not isinstance(b, dict):
            continue
        btype = (b.get('@type') or '').lower()
        if btype == 'imagegallery' or b.get('associatedMedia'):
            am = b.get('associatedMedia') or []
            if isinstance(am, list) and am:
                first = am[0]
                if isinstance(first, dict):
                    return first.get('contentUrl') or first.get('url')
    return None


def find_product_block(blocks):
    for b in blocks:
        if not isinstance(b, dict):
            continue
        t = (b.get('@type') or '').lower()
        if t == 'product' or t.endswith('product'):
            return b
    for b in blocks:
        if isinstance(b, dict):
            for v in b.values():
                if isinstance(v, dict) and (v.get('@type') or '').lower() == 'product':
                    return v
    return None


def extract_price_from_product(prod):
    offers = prod.get('offers')
    if isinstance(offers, list) and offers:
        of = offers[0]
        return of.get('price'), of.get('priceCurrency')
    if isinstance(offers, dict):
        return offers.get('price'), offers.get('priceCurrency')
    return None, None


def extract_details_text(html):
    soup = BeautifulSoup(html, "lxml")
    texts = []
    selectors = [
        ".as-productinfo", ".product-hero", ".tech-specs", ".rb-content", ".product-hero__description",
        ".section-copy", "#overview", ".description"
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            texts.append(node.get_text(" ", strip=True))
    if not texts:
        desc = meta_tag(html, name='description') or meta_tag(html, prop='og:description')
        if desc:
            texts.append(desc)
    return "\n".join(texts).strip()


def find_ram_storage_chip(text):
    res = {'ram': None, 'storage': None, 'chip': None}
    if not text:
        return res
    t = text.replace('\xa0', ' ').replace('Go', 'GB')
    sizes = re.findall(r'(\d{1,4}\s?(?:GB|TB))', t, flags=re.IGNORECASE)
    if sizes:
        res['storage'] = sizes[0].upper().replace(' ', '')
        if len(sizes) > 1:
            res['ram'] = sizes[1].upper().replace(' ', '')
    chip_m = re.search(r'(A\d+\s*Bionic|S\d+\s*SiP|M\d+(?:\s*(?:Pro|Max|Ultra))?|Apple\s+M\d+|P\d+|A\d+ Bionic)',
                       t, flags=re.IGNORECASE)
    if chip_m:
        res['chip'] = chip_m.group(1).strip()
    else:
        chip2 = re.search(r'Puce\s*[:\-]?\s*([A-Za-z0-9\s\-]+)|chip\s*[:\-]?\s*([A-Za-z0-9\s\-]+)',
                          t, flags=re.IGNORECASE)
        if chip2:
            res['chip'] = (chip2.group(1) or chip2.group(2) or "").strip()
    return res


def parse_product_page_html(html, source_url=None):
    blocks = extract_ld_json(html)
    prod_block = find_product_block(blocks)
    title = None
    price = None
    currency = None
    image = None
    description = None
    additional_details = extract_details_text(html)

    if prod_block:
        title = prod_block.get('name') or prod_block.get('headline')
        price, currency = extract_price_from_product(prod_block)
        image = prod_block.get('image')
        if isinstance(image, list):
            image = image[0]
        if not image:
            image = find_first_image_from_imagegallery(blocks) or meta_tag(html, prop='og:image')
        description = prod_block.get('description') or meta_tag(html, prop='og:description') or meta_tag(html, name='description')
    else:
        title = meta_tag(html, prop='og:title') or meta_tag(html, name='title') or source_url
        image = meta_tag(html, prop='og:image')
        description = meta_tag(html, prop='og:description') or meta_tag(html, name='description')
        mprice = re.search(r'"priceCurrency"\s*:\s*["\']([A-Z]{3})["\'].*?"price"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
                           html, flags=re.IGNORECASE | re.DOTALL)
        if mprice:
            currency = mprice.group(1)
            try:
                price = float(mprice.group(2))
            except:
                price = mprice.group(2)
        else:
            mcur = re.search(r'"currentPrice"\s*:\s*\{[^}]*"raw_amount"\s*:\s*["\']([0-9\.]+)["\']', html)
            if mcur:
                try:
                    price = float(mcur.group(1))
                except:
                    price = mcur.group(1)

    longtext = (description or '') + "\n" + additional_details
    specs = find_ram_storage_chip(longtext)

    is_product = False
    if prod_block:
        is_product = True
    elif price is not None:
        is_product = True
    elif image and (specs['storage'] or specs['chip'] or specs['ram']):
        is_product = True
    else:
        og_type = meta_tag(html, prop='og:type')
        if og_type and 'product' in og_type.lower():
            is_product = True

    parsed = {
        "title": (title.strip() if title else None),
        "price": price,
        "currency": currency,
        "ram": specs['ram'],
        "storage": specs['storage'],
        "chip": specs['chip'],
        "additional_details": additional_details,
        "image": image,
        "source_url": source_url,
        "is_product": is_product
    }
    return parsed


# ---------- Link discovery & heuristics ----------
def looks_like_product_url(url: str) -> bool:
    """
    Stronger product URL heuristics:
    - canonical pattern: /shop/product/
    - fallback patterns: /product/ or /A/<ID segment> (Apple product IDs)
    - some refurbished product anchors live under /shop/refurbished/... but contain product IDs or fnode query;
      accept conservatively only when those patterns present.
    """
    if re.search(r'/shop/product/', url, flags=re.IGNORECASE):
        return True
    if re.search(r'/product/|/product-page|/A/[A-Z0-9]{5,}', url, flags=re.IGNORECASE):
        return True
    if '/shop/refurbished/' in url and ('?fnode=' in url or re.search(r'/[A-Z0-9]{6,}', url)):
        return True
    return False


async def discover_links_on_page(page, base_url):
    """
    Return same-origin links on the page (normalized, fragments removed).
    Caller will split into product-like vs category-like.
    """
    urls = set()
    anchors = await page.query_selector_all("a[href]")
    for a in anchors:
        try:
            href = await a.get_attribute("href")
            if not href or href.startswith("javascript:") or href.startswith("#"):
                continue
            full = urljoin(base_url, href)
            parsed_base = urlparse(base_url)
            parsed_full = urlparse(full)
            if parsed_base.netloc != parsed_full.netloc:
                continue
            urls.add(full.split('#')[0].rstrip('/'))
        except Exception:
            continue

    # JSON-LD fallback
    content = await page.content()
    blocks = extract_ld_json(content)
    for b in blocks:
        if isinstance(b, dict):
            u = b.get('url') or b.get('@id') or b.get('mainEntityOfPage')
            if isinstance(u, str):
                urls.add(u.split('#')[0].rstrip('/'))

    return urls


# ---------- Validate & parse ----------
async def fetch_and_validate_parse(semaphore: Semaphore, browser, url: str, verbose=False):
    """
    Fetch the candidate URL and parse/validate productness in a single fetch.
    Return parsed dict (with source_url) if product, else None.
    """
    async with semaphore:
        context = await browser.new_context(user_agent="Mozilla/5.0 (compatible; RefurbCrawler/1.0)")
        page = await context.new_page()
        try:
            await page.goto(url, timeout=PRODUCT_PAGE_TIMEOUT)
            await asyncio.sleep(random.uniform(*RANDOM_DELAY))
            html = await page.content()
            parsed = parse_product_page_html(html, source_url=url)
            if parsed and parsed.get("is_product"):
                parsed.pop("is_product", None)
                if verbose:
                    print(json.dumps(parsed, ensure_ascii=False))
                await page.close()
                await context.close()
                return parsed
            else:
                if verbose:
                    print(f"  - skipping non-product: {url}")
                await page.close()
                await context.close()
                return None
        except Exception as e:
            if verbose:
                print(f"  ! failed validating: {url} -> {e}")
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            return None


# ---------- BFS crawler ----------
async def try_expand_listing(page):
    clicked_any = False
    more_text_patterns = [
        r'load more', r'show more', r'mehr', r'more results', r'anzeigen', r'voir plus', r'voir plus de',
        r'carica altro', r'cargar más', r'cargar mas', r'zeige mehr', r'view more'
    ]
    try:
        for _ in range(6):
            btn = await page.query_selector("button:has-text('Load more'), button:has-text('Show more'), button:has-text('Mehr')")
            if btn:
                try:
                    await btn.scroll_into_view_if_needed()
                    await btn.click(timeout=PAGE_TIMEOUT)
                    clicked_any = True
                    await asyncio.sleep(random.uniform(*RANDOM_DELAY))
                    continue
                except Exception:
                    pass
            buttons = await page.query_selector_all("button")
            found = False
            for b in buttons:
                try:
                    txt = (await b.inner_text()).strip().lower()
                    for pat in more_text_patterns:
                        if re.search(pat, txt, flags=re.IGNORECASE):
                            await b.scroll_into_view_if_needed()
                            await b.click(timeout=PAGE_TIMEOUT)
                            clicked_any = True
                            found = True
                            await asyncio.sleep(random.uniform(*RANDOM_DELAY))
                            break
                    if found:
                        break
                except Exception:
                    continue
            if not found:
                await page.evaluate("() => window.scrollBy(0, document.body.scrollHeight)")
                await asyncio.sleep(0.8)
                break
    except PlaywrightTimeoutError:
        pass
    except Exception:
        pass
    return clicked_any


async def crawl_country_bfs(country_tag, start_url, browser, max_per_country=0, max_pages=0, verbose=False):
    print(f"[+] {country_tag} -> {start_url}")
    page = await browser.new_page()
    try:
        await page.goto(start_url, timeout=PAGE_TIMEOUT)
        await asyncio.sleep(random.uniform(*RANDOM_DELAY))
    except Exception as e:
        print(f"  ! failed to open start page: {e}")
        try:
            await page.close()
        except:
            pass
        return []

    # expand the start page to reveal category links
    await try_expand_listing(page)
    initial_links = await discover_links_on_page(page, start_url)
    await page.close()

    # seed only refurbished category links (avoid seeding everything)
    category_links = [u for u in initial_links
                      if '/shop/refurbished/' in u and u.rstrip('/') != start_url.rstrip('/')]
    if not category_links:
        # fallback: if we found no explicit category link, include start page itself
        queue = [(start_url.rstrip('/'), 1)]
    else:
        queue = [(u.rstrip('/'), 1) for u in category_links]

    visited = set()
    results = []
    pages_visited = 0
    sem = Semaphore(CONCURRENT_PRODUCT_FETCHES)
    product_tasks = []

    max_products_cap = max_per_country or DEFAULT_MAX_PRODUCTS_PER_COUNTRY
    max_pages_cap = max_pages or DEFAULT_MAX_PAGES_PER_COUNTRY

    while queue and len(results) < max_products_cap and pages_visited < max_pages_cap:
        url, depth = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        # keep same host
        if urlparse(url).netloc != urlparse(start_url).netloc:
            continue

        # avoid re-visiting start root
        if url.rstrip('/') == start_url.rstrip('/'):
            # still process once if it was our only seed
            pass

        # open page
        try:
            page = await browser.new_page()
            await page.goto(url, timeout=PAGE_TIMEOUT)
            await asyncio.sleep(random.uniform(*RANDOM_DELAY))
            pages_visited += 1
            html = await page.content()

            # check quick heuristics for product
            blocks = extract_ld_json(html)
            prod_block = find_product_block(blocks)
            og_type = meta_tag(html, prop='og:type') or ""
            maybe_product_url = looks_like_product_url(url)

            if prod_block or 'product' in og_type.lower() or maybe_product_url:
                # schedule validation and parsing (single fetch inside function)
                task = asyncio.create_task(fetch_and_validate_parse(sem, browser, url, verbose=verbose))
                product_tasks.append(task)
            else:
                # treat as category/listing — expand and discover inner links
                await try_expand_listing(page)
                inner_links = await discover_links_on_page(page, url)

                # split discovered links into product-like and category-like
                product_like = [u for u in inner_links if looks_like_product_url(u)]
                category_like = [u for u in inner_links if '/shop/refurbished/' in u and not looks_like_product_url(u)]

                # prioritize product links
                for pl in product_like:
                    if pl not in visited:
                        queue.insert(0, (pl, depth))  # immediate
                # enqueue subcategories (increase depth)
                if depth < MAX_BFS_DEPTH:
                    for cl in category_like:
                        if cl not in visited:
                            queue.append((cl, depth + 1))

                if verbose:
                    print(f"  [BFS] visited {url} depth={depth} -> links={len(inner_links)} products={len(product_like)} categories={len(category_like)}")
            await page.close()
        except Exception as e:
            if verbose:
                print(f"  ! failed visiting {url}: {e}")
            try:
                await page.close()
            except:
                pass
            continue

        # harvest completed product tasks (non-blocking)
        if product_tasks:
            done, pending = await asyncio.wait(product_tasks, timeout=0, return_when=asyncio.ALL_COMPLETED)
            product_tasks = list(pending)
            for d in done:
                try:
                    parsed = d.result()
                    if parsed:
                        results.append(parsed)
                        if len(results) >= max_products_cap:
                            break
                except Exception:
                    pass

    # finish pending product tasks
    if product_tasks and len(results) < max_products_cap:
        done, pending = await asyncio.wait(product_tasks, return_when=asyncio.ALL_COMPLETED)
        for d in done:
            try:
                parsed = d.result()
                if parsed:
                    results.append(parsed)
                    if len(results) >= max_products_cap:
                        break
            except Exception:
                pass

    print(f"[+] {country_tag}: parsed {len(results)} products (pages visited: {pages_visited})")
    return results


# ---------- CLI & main ----------
async def main_async(args):
    if args.start_urls:
        start_map = {}
        with open(args.start_urls, "r", encoding="utf-8") as fh:
            for ln in fh:
                s = ln.strip()
                if not s:
                    continue
                if "\t" in s:
                    tag, url = [x.strip() for x in s.split("\t", 1)]
                elif " " in s and s.split()[0].isalpha() and len(s.split()[0]) <= 3:
                    parts = s.split(None, 1)
                    tag, url = parts[0], parts[1] if len(parts) > 1 else s
                else:
                    parsed = urlparse(s)
                    host = parsed.netloc or "unknown"
                    tag = host.split(".")[0].upper()
                    url = s
                start_map[tag.upper()] = url
    else:
        start_map = dict(DEFAULT_COUNTRY_START_URLS)

    if args.countries:
        wanted = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
        start_map = {k: v for k, v in start_map.items() if k in wanted}

    if not start_map:
        print("No start URLs. Provide --start-urls or update defaults.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.show_browser)
        out = {}
        for tag, url in start_map.items():
            prods = await crawl_country_bfs(tag, url, browser, max_per_country=(args.max_per_country or 0),
                                            max_pages=(args.max_pages or 0), verbose=args.verbose)
            out[tag] = prods
            time.sleep(random.uniform(*RANDOM_DELAY))

        out_path = Path(args.output)
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Saved:", out_path.resolve())
        await browser.close()


def parse_cli():
    p = argparse.ArgumentParser()
    p.add_argument("--countries", help="Comma separated country codes (use keys of default map)", default=None)
    p.add_argument("--start-urls", help="File with start URLs (one per line). If provided, overrides defaults", default=None)
    p.add_argument("--output", help="Output JSON path", default="refurbs_by_country_playwright.json")
    p.add_argument("--max-per-country", type=int, default=0, help="Limit products per country (0=unlimited)")
    p.add_argument("--max-pages", type=int, default=0, help="Limit listing/category pages per country (0=default cap)")
    p.add_argument("--verbose", action="store_true", help="Print parsed product objects as they are parsed and BFS logs")
    p.add_argument("--show-browser", action="store_true", help="Show browser window (non-headless) for debugging")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_cli()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("Interrupted by user")
