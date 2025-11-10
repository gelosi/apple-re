#!/usr/bin/env python3
"""
apple_refurbs_playwright.py

Improved Playwright crawler for Apple refurbished storefronts.
- Better multilingual support for specs extraction
- Improved chip detection with deduplication and validation
- Enhanced RAM detection for multiple languages
- More robust parsing logic
"""

import asyncio
import argparse
import json
import re
import random
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import Set, List, Tuple, Optional, Dict

from bs4 import BeautifulSoup
from html import unescape
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from asyncio import Semaphore

# ---------- Defaults ----------
DEFAULT_COUNTRY_START_URLS = {
    "US": "https://www.apple.com/shop/refurbished",
    "CA": "https://www.apple.com/ca/shop/refurbished",
    "GB": "https://www.apple.com/uk/shop/refurbished",
    "DE": "https://www.apple.com/de/shop/refurbished",
    "FR": "https://www.apple.com/fr/shop/refurbished",
    "ES": "https://www.apple.com/es/shop/refurbished",
    "IT": "https://www.apple.com/it/shop/refurbished",
    "NL": "https://www.apple.com/nl/shop/refurbished",
    "SE": "https://www.apple.com/se/shop/refurbished",
    "CHDE": "https://www.apple.com/ch-de/shop/refurbished",
    "IE": "https://www.apple.com/ie/shop/refurbished"
}

RANDOM_DELAY = (0.2, 1.0)
PAGE_TIMEOUT = 30000
PRODUCT_PAGE_TIMEOUT = 30000
CONCURRENT_PRODUCT_FETCHES = 6

DEFAULT_MAX_PAGES_PER_COUNTRY = 200
DEFAULT_MAX_PRODUCTS_PER_COUNTRY = 1000
MAX_BFS_DEPTH = 3


class AppleSpecDetector:
    """Enhanced Apple CPU and RAM detection with multilingual support and deduplication"""
    
    def __init__(self):
        # Valid Apple CPUs with their canonical names (no A26, A28 etc.)
        self.valid_cpus = {
            # A-series (iPhone/iPad)
            'A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'A10', 'A10X', 
            'A11', 'A12', 'A12X', 'A12Z', 'A13', 'A14', 'A15', 
            'A16', 'A17', 'A17 Pro', 'A18', 'A18 Pro',
            
            # M-series (Mac)
            'M1', 'M1 Pro', 'M1 Max', 'M1 Ultra',
            'M2', 'M2 Pro', 'M2 Max', 'M2 Ultra', 
            'M3', 'M3 Pro', 'M3 Max', 'M3 Ultra',
            'M4', 'M4 Pro', 'M4 Max', 'M4 Ultra',
            
            # S-series (Watch) - SiP (System in Package)
            'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'S10'
        }
        
        self.cpu_patterns = self._create_cpu_patterns()
        
    def _create_cpu_patterns(self) -> List[Tuple[re.Pattern, str]]:
        """Create regex patterns to match and normalize CPU names"""
        patterns = []
        
        for cpu in self.valid_cpus:
            # Create patterns for different formats
            pattern_variations = [
                # "Apple M4 Chip", "Apple M4 chip", "AppleÂ M4Â Chip"
                rf'Apple\s*(?:Â\s*)?{re.escape(cpu)}\s*(?:Â\s*)?\s*[Cc]hip',
                # "M4 Chip", "M4 chip"  
                rf'\b{re.escape(cpu)}\s+[Cc]hip\b',
                # Just "M4", "A17", etc. with word boundaries
                rf'\b{re.escape(cpu)}\b(?!\s*[Cc]hip)',
            ]
            
            # Special handling for SiP (System in Package)
            if cpu.startswith('S') and cpu[1:].isdigit():
                pattern_variations.extend([
                    rf'\b{re.escape(cpu)}\s+SiP\b',
                    rf'\b{re.escape(cpu)}\s+SIP\b',
                ])
            
            # Special handling for Bionic
            if cpu.startswith('A') and len(cpu.replace(' Pro', '')) <= 3:
                pattern_variations.append(rf'\b{re.escape(cpu)}\s+Bionic\b')
            
            for pattern_str in pattern_variations:
                pattern = re.compile(pattern_str, re.IGNORECASE)
                patterns.append((pattern, cpu))
                
        return patterns
    
    def detect_cpus(self, text: str) -> Set[str]:
        """Detect and normalize CPU names from text"""
        if not text:
            return set()
            
        detected_cpus = set()
        
        for pattern, canonical_name in self.cpu_patterns:
            if pattern.search(text):
                detected_cpus.add(canonical_name)
                
        return detected_cpus
    
    def detect_ram(self, text: str) -> Set[str]:
        """Detect RAM specifications from multilingual text"""
        if not text:
            return set()
            
        # Handle special characters and normalize
        normalized_text = text.replace('\xa0', ' ').replace('Â', ' ')
        
        ram_patterns = [
            # English patterns
            r'(\d+)\s*GB\s+(?:unified\s+)?memory\b',
            r'(\d+)\s*GB\s+(?:of\s+)?(?:unified\s+)?memory\b',
            r'(\d+)\s*GB\s+RAM\b',
            r'(\d+)\s*GB\s+(?:of\s+)?RAM\b',
            
            # German patterns (like your sample)
            r'(\d+)\s*GB\s+gemeinsamer\s+Arbeitsspeicher\b',
            r'(\d+)\s*GB\s+Arbeitsspeicher\b',
            
            # French patterns
            r'(\d+)\s*Go\s+de\s+mémoire\s+unifiée\b',
            r'(\d+)\s*Go\s+de\s+mémoire\b',
            
            # Spanish patterns
            r'(\d+)\s*GB\s+de\s+memoria\s+unificada\b',
            r'(\d+)\s*GB\s+de\s+memoria\b',
            
            # Italian patterns
            r'(\d+)\s*GB\s+di\s+memoria\s+unificata\b',
            r'(\d+)\s*GB\s+di\s+memoria\b',
            
            # Dutch patterns
            r'(\d+)\s*GB\s+(?:gedeeld\s+)?geheugen\b',
            
            # General memory patterns
            r'memory[:\s]*(\d+)\s*GB\b',
            r'RAM[:\s]*(\d+)\s*GB\b',
        ]
        
        detected_ram = set()
        
        for pattern in ram_patterns:
            matches = re.finditer(pattern, normalized_text, re.IGNORECASE)
            for match in matches:
                gb_value = int(match.group(1))
                # Only accept realistic RAM values (4GB to 192GB for Apple devices)
                if 4 <= gb_value <= 192:
                    detected_ram.add(f"{gb_value}GB")
                    
        return detected_ram


# Global detector instance
spec_detector = AppleSpecDetector()


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


def get_canonical(html):
    # try og:url, then link rel=canonical, then <meta itemprop="url">
    og = meta_tag(html, prop='og:url')
    if og:
        return og.split('#')[0].rstrip('/')
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    if m:
        return m.group(1).split('#')[0].rstrip('/')
    item = re.search(r'<meta[^>]+itemprop=["\']url["\'][^>]*content=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    if item:
        return item.group(1).split('#')[0].rstrip('/')
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
        ".section-copy", "#overview", ".description", ".rf-configuration-subheader",
        ".rf-configuration-productsummary"
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


def detect_product_category(title: str, url: str = None) -> str:
    """Detect product category from title and URL."""
    if not title:
        return "Other"
    
    t = title.lower()
    u = (url or "").lower()
    
    # Desktop computers - check specific models first to avoid conflicts
    if any(x in t for x in ['mac mini', 'mac studio', 'mac pro', 'imac']):
        return "Desktop"
    
    # Laptops - check after desktop to avoid "mac" conflicts
    if any(x in t for x in ['macbook', 'mac book']):
        return "Laptop"
    
    # iPads
    if 'ipad' in t:
        return "iPad"
    
    # iPhones
    if 'iphone' in t:
        return "iPhone"
    
    # Apple Watch
    if any(x in t for x in ['watch', 'apple watch']):
        return "Watch"
    
    # Apple TV
    if 'apple tv' in t or 'appletv' in t:
        return "Apple TV"
    
    # HomePod
    if 'homepod' in t:
        return "HomePod"
    
    # AirPods
    if 'airpod' in t:
        return "AirPods"
    
    # Accessories
    if any(x in t for x in ['pencil', 'keyboard', 'mouse', 'trackpad', 'cable', 'adapter', 
                            'magsafe', 'power', 'case', 'band', 'strap', 'studio display']):
        return "Accessory"
    
    # Check URL patterns as fallback
    if u:
        if '/mac/' in u or '/macbook' in u:
            if 'book' in u:
                return "Laptop"
            else:
                return "Desktop"
        if '/ipad' in u:
            return "iPad"
        if '/iphone' in u:
            return "iPhone"
        if '/watch' in u:
            return "Watch"
        if '/tv' in u:
            return "Apple TV"
        if '/airpods' in u:
            return "AirPods"
        if '/homepod' in u:
            return "HomePod"
    
    return "Other"


def detect_storage(text: str) -> Optional[str]:
    """Detect storage from multilingual text"""
    if not text:
        return None
        
    # Normalize text
    normalized_text = text.replace('\xa0', ' ').replace('Â', ' ')
    
    storage_patterns = [
        # English
        r'(\d{1,4})\s*([GT])B\s+(?:of\s+)?(?:SSD|storage|Storage)\b',
        r'(?:SSD|storage|Storage)[\s:]+(\d{1,4})\s*([GT])B\b',
        
        # German (like your sample: "512 GB SSD Speicher")
        r'(\d{1,4})\s*([GT])B\s+SSD\s+Speicher\b',
        r'(\d{1,4})\s*([GT])B\s+Speicher\b',
        
        # French
        r'(\d{1,4})\s*([GT])o\s+(?:de\s+)?(?:SSD|stockage)\b',
        r'(?:SSD|stockage)[\s:]+(?:de\s+)?(\d{1,4})\s*([GT])o\b',
        
        # Spanish
        r'(\d{1,4})\s*([GT])B\s+(?:de\s+)?(?:SSD|almacenamiento)\b',
        
        # Italian
        r'(\d{1,4})\s*([GT])B\s+(?:di\s+)?(?:SSD|archiviazione)\b',
        
        # General pattern for large standalone numbers (likely storage)
        r'\b(\d{3,4})\s*([GT])B\b(?!\s*(?:memory|Memory|RAM|mémoire|memoria|Arbeitsspeicher))',
    ]
    
    for pattern in storage_patterns:
        match = re.search(pattern, normalized_text, re.IGNORECASE)
        if match:
            size_num = int(match.group(1))
            unit = match.group(2).upper()
            
            # Filter out unrealistic values
            if unit == 'G' and size_num >= 128:  # Minimum 128GB
                return f"{size_num}GB"
            elif unit == 'T' and 1 <= size_num <= 8:  # 1-8TB range
                return f"{size_num}TB"
                
    return None


def find_ram_storage_chip(text: str, title: str = None):
    """Improved detection using the new spec detector"""
    res = {'ram': None, 'storage': None, 'chip': None}
    if not text:
        return res

    # Combine title and text for better detection
    combined_text = (title or '') + " " + text
    
    # Use the improved spec detector
    detected_cpus = spec_detector.detect_cpus(combined_text)
    detected_ram = spec_detector.detect_ram(combined_text)
    
    # Take the first/best CPU and RAM
    if detected_cpus:
        # Prefer M-series over others, then by recency
        cpu_list = sorted(detected_cpus, key=lambda x: (
            0 if x.startswith('M4') else
            1 if x.startswith('M3') else
            2 if x.startswith('M2') else
            3 if x.startswith('M1') else
            4 if x.startswith('A') else 5
        ))
        res['chip'] = cpu_list[0]
    
    if detected_ram:
        # Take the largest RAM size if multiple found
        ram_list = sorted(detected_ram, key=lambda x: int(x.replace('GB', '')), reverse=True)
        res['ram'] = ram_list[0]
    
    # Use separate storage detection
    storage = detect_storage(combined_text)
    if storage:
        res['storage'] = storage
    
    # Special case: Accessories shouldn't have chip info
    if title and any(x in title.lower() for x in ['pencil', 'keyboard', 'mouse', 'cable', 'adapter']):
        res['chip'] = None
    
    return res


def parse_product_page_html(html, source_url=None):
    """Parse fields and return dict including 'is_product' boolean and category."""
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

    # Extract specs with improved multilingual support
    longtext = (description or '') + "\n" + additional_details
    specs = find_ram_storage_chip(longtext, title)
    
    # Detect product category
    category = detect_product_category(title, source_url)

    # decide product-ness conservatively
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
    
    # Don't treat error pages as products
    if title and any(x in title.lower() for x in ['page not found', 'page introuvable', 'no se encuentra', 'nicht gefunden']):
        is_product = False

    parsed = {
        "title": (title.strip() if title else None),
        "category": category,
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
    """Strong product URL heuristics: canonical /shop/product/ or product-id segments."""
    if re.search(r'/shop/product/', url, flags=re.IGNORECASE):
        return True
    if re.search(r'/product/|/product-page|/A/[A-Z0-9]{5,}', url, flags=re.IGNORECASE):
        return True
    # Accept refurbished links that embed an uppercase product token or fnode param conservatively
    if '/shop/refurbished/' in url and ('?fnode=' in url or re.search(r'/[A-Z0-9]{6,}', url)):
        return True
    return False


async def discover_links_on_page(page, base_url):
    """Return same-origin normalized links found on the page."""
    urls = set()
    anchors = await page.query_selector_all("a[href]")
    for a in anchors:
        try:
            href = await a.get_attribute("href")
            if not href or href.startswith("javascript:") or href.startswith("#"):
                continue
            full = urljoin(base_url, href)
            # same host only
            if urlparse(full).netloc != urlparse(base_url).netloc:
                continue
            urls.add(full.split('#')[0].rstrip('/'))
        except Exception:
            continue

    content = await page.content()
    blocks = extract_ld_json(content)
    for b in blocks:
        if isinstance(b, dict):
            u = b.get('url') or b.get('@id') or b.get('mainEntityOfPage')
            if isinstance(u, str):
                urls.add(u.split('#')[0].rstrip('/'))

    return urls


# ---------- Validate & parse (single fetch) ----------
async def fetch_and_validate_parse(semaphore: Semaphore, browser, url: str, verbose=False):
    """Fetch the candidate URL, parse and validate it."""
    async with semaphore:
        context = await browser.new_context(user_agent="Mozilla/5.0 (compatible; RefurbCrawler/1.0)")
        page = await context.new_page()
        try:
            await page.goto(url, timeout=PRODUCT_PAGE_TIMEOUT)
            await asyncio.sleep(random.uniform(*RANDOM_DELAY))
            html = await page.content()
            canonical = get_canonical(html)
            # choose validation URL: prefer canonical if it's product-like
            validate_url = url
            if canonical and looks_like_product_url(canonical):
                validate_url = canonical
            parsed = parse_product_page_html(html, source_url=validate_url)
            if parsed and parsed.get("is_product"):
                parsed.pop("is_product", None)
                # if canonical exists and looks product-like, set source_url to canonical
                if canonical and looks_like_product_url(canonical):
                    parsed['source_url'] = canonical
                else:
                    parsed['source_url'] = url
                if verbose:
                    print(json.dumps(parsed, ensure_ascii=False))
                await page.close()
                await context.close()
                return parsed
            else:
                if verbose:
                    print(f"  - skipping non-product: {url} (canonical: {canonical})")
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

    # expand and seed only category links under /shop/refurbished/
    await try_expand_listing(page)
    initial_links = await discover_links_on_page(page, start_url)
    await page.close()

    category_links = [u for u in initial_links
                      if '/shop/refurbished/' in u and u.rstrip('/') != start_url.rstrip('/')]
    if not category_links:
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

        if urlparse(url).netloc != urlparse(start_url).netloc:
            continue

        try:
            page = await browser.new_page()
            await page.goto(url, timeout=PAGE_TIMEOUT)
            await asyncio.sleep(random.uniform(*RANDOM_DELAY))
            pages_visited += 1
            html = await page.content()

            # canonical and quick checks
            canonical = get_canonical(html)
            prod_block = find_product_block(extract_ld_json(html))
            og_type = meta_tag(html, prop='og:type') or ""

            url_is_product_like = looks_like_product_url(url)
            canonical_is_product_like = bool(canonical and looks_like_product_url(canonical))

            # Only treat this visited URL as a candidate product if the URL (or canonical) is product-like.
            if url_is_product_like or canonical_is_product_like:
                validate_source = canonical if canonical_is_product_like else url
                parsed = parse_product_page_html(html, source_url=validate_source)
                if parsed.get("is_product"):
                    parsed.pop("is_product", None)
                    # prefer canonical as source_url when it looks product-like
                    if canonical_is_product_like:
                        parsed['source_url'] = canonical
                    else:
                        parsed['source_url'] = url
                    results.append(parsed)
                    if verbose:
                        print(json.dumps(parsed, ensure_ascii=False))
                else:
                    if verbose:
                        print(f"  - page looked product-like by URL but parsed as non-product: {url} (canonical: {canonical})")
                await page.close()
            else:
                # Category/listing page: expand then discover inner links
                await try_expand_listing(page)
                inner_links = await discover_links_on_page(page, url)

                # split discovered links into product-like and category-like
                product_like = [u for u in inner_links if looks_like_product_url(u)]
                category_like = [u for u in inner_links if '/shop/refurbished/' in u and not looks_like_product_url(u)]

                # prioritize product links (schedule fetch+validate)
                for pl in product_like:
                    if pl not in visited:
                        # schedule concurrent validation-only fetch for pl
                        task = asyncio.create_task(fetch_and_validate_parse(sem, browser, pl, verbose=verbose))
                        product_tasks.append(task)

                # enqueue subcategories (increase depth)
                if depth < MAX_BFS_DEPTH:
                    for cl in category_like:
                        if cl not in visited:
                            queue.append((cl, depth + 1))

                if verbose:
                    print(f"  [BFS] visited {url} depth={depth} -> links={len(inner_links)} product_like={len(product_like)} category_like={len(category_like)}")
                await page.close()
        except Exception as e:
            if verbose:
                print(f"  ! failed visiting {url}: {e}")
            try:
                await page.close()
            except:
                pass
            continue

        # harvest any finished product tasks quickly
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

    # finish any remaining product tasks
    if product_tasks and len(results) < max_products_cap:
        done, _ = await asyncio.wait(product_tasks, return_when=asyncio.ALL_COMPLETED)
        for d in done:
            try:
                parsed = d.result()
                if parsed:
                    results.append(parsed)
                    if len(results) >= max_products_cap:
                        break
            except Exception:
                pass

    # Filter out error pages and deduplicate
    results = [r for r in results if r.get('title') and 
               not any(x in r['title'].lower() for x in ['page not found', 'page introuvable', 'no se encuentra', 'nicht gefunden'])]
    
    # Remove duplicates based on source_url
    seen_urls = set()
    unique_results = []
    for result in results:
        if result['source_url'] not in seen_urls:
            seen_urls.add(result['source_url'])
            unique_results.append(result)

    print(f"[+] {country_tag}: parsed {len(unique_results)} unique products (pages visited: {pages_visited})")
    return unique_results


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
        
        # Summary stats for debugging
        total_products = 0
        cpu_stats = {}
        ram_stats = {}
        
        for tag, url in start_map.items():
            prods = await crawl_country_bfs(tag, url, browser, max_per_country=(args.max_per_country or 0),
                                            max_pages=(args.max_pages or 0), verbose=args.verbose)
            out[tag] = prods
            total_products += len(prods)
            
            # Collect stats for debugging
            for prod in prods:
                if prod.get('chip'):
                    cpu_stats[prod['chip']] = cpu_stats.get(prod['chip'], 0) + 1
                if prod.get('ram'):
                    ram_stats[prod['ram']] = ram_stats.get(prod['ram'], 0) + 1
                    
            time.sleep(random.uniform(*RANDOM_DELAY))

        out_path = Path(args.output)
        out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        
        print(f"\n[SUMMARY]")
        print(f"Total products: {total_products}")
        print(f"Unique CPUs found: {len(cpu_stats)}")
        print(f"Unique RAM configs: {len(ram_stats)}")
        
        if args.verbose and cpu_stats:
            print(f"\nCPU distribution:")
            for cpu, count in sorted(cpu_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"  {cpu}: {count}")
                
        if args.verbose and ram_stats:
            print(f"\nRAM distribution:")
            for ram, count in sorted(ram_stats.items(), key=lambda x: int(x[0].replace('GB', '')))[:10]:
                print(f"  {ram}: {count}")
        
        print(f"\nSaved: {out_path.resolve()}")
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