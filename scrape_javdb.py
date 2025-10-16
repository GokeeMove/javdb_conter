#!/usr/bin/env python3
import argparse
import csv
import json
import os
import random
import sys
import time
import logging
from time import perf_counter
from dataclasses import dataclass, asdict, field
from typing import Dict, Iterable, List, Optional, Tuple

import chardet
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "upgrade-insecure-requests": "1",
}


def build_session(user_agent: Optional[str], cookies: Optional[str], proxy: Optional[str], timeout: int) -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=32)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    headers = dict(DEFAULT_HEADERS)
    if user_agent:
        headers["user-agent"] = user_agent
    else:
        headers["user-agent"] = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/127.0.0.0 Safari/537.36"
        )
    session.headers.update(headers)

    if cookies:
        # Accept raw cookie string: "a=b; c=d"
        for part in cookies.split(";"):
            if not part.strip():
                continue
            if "=" in part:
                name, value = part.split("=", 1)
                session.cookies.set(name.strip(), value.strip())

    if proxy:
        session.proxies.update({
            "http": proxy,
            "https": proxy,
        })

    # Attach default timeout to session via request wrapper
    original_request = session.request

    def request_with_timeout(method: str, url: str, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return original_request(method, url, **kwargs)

    session.request = request_with_timeout  # type: ignore[assignment]
    return session


def detect_encoding(content: bytes) -> str:
    guess = chardet.detect(content)
    return guess.get("encoding") or "utf-8"


def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    t0 = perf_counter()
    logging.debug(f"HTTP GET -> {url}")
    resp = session.get(url)
    dt = (perf_counter() - t0) * 1000
    logging.debug(f"HTTP {resp.status_code} <- {url} ({len(resp.content)} bytes in {dt:.1f} ms)")
    resp.raise_for_status()
    encoding = resp.apparent_encoding or detect_encoding(resp.content)
    html = resp.content.decode(encoding, errors="ignore")
    return BeautifulSoup(html, "lxml")


@dataclass
class JavdbItem:
    title: str
    code: Optional[str]
    date: Optional[str]
    score: Optional[str]
    actors: List[str]
    categories: List[str]
    cover_url: Optional[str]
    detail_url: Optional[str]
    # translated fields (optional)
    title_zh: Optional[str] = None
    actors_zh: Optional[List[str]] = None
    categories_zh: Optional[List[str]] = None
    # detail fields
    magnets: List[str] = field(default_factory=list)


def text_or_none(node) -> Optional[str]:
    if not node:
        return None
    text = node.get_text(strip=True)
    return text or None


def parse_listing_items(soup: BeautifulSoup, base_url: str) -> List[JavdbItem]:
    items: List[JavdbItem] = []

    # Try common containers
    containers = []
    containers.extend(soup.select("div.item"))
    containers.extend(soup.select("div.movie-box"))
    containers.extend(soup.select("article"))

    seen = set()
    for c in containers:
        # title and detail link
        a = c.select_one("a[href]")
        if not a:
            continue
        href = a.get("href")
        if not href:
            continue
        detail_url = href if href.startswith("http") else _join_url(base_url, href)

        # cover image
        img = a.select_one("img") or c.select_one("img")
        cover_url = None
        if img:
            cover_url = img.get("data-src") or img.get("src")

        # Extract title candidates
        title_node = (
            c.select_one(".video-title, .title, h3, h4, .name") or a
        )
        title = text_or_none(title_node) or ""

        # Extract code/id if visible
        code = None
        # Common patterns: span:contains("JAV-123"), small code under title, badge
        code_candidates = []
        code_candidates.extend(c.select(".uid, .code, .video-code, .badge"))
        code_candidates.extend(c.select("small"))
        code_candidates.extend(c.select(".meta, .subtitle"))
        for node in code_candidates:
            t = text_or_none(node)
            if not t:
                continue
            # Heuristic: contains dash + digits or uppercase letters+digits
            if any(ch.isdigit() for ch in t) and any(ch.isalpha() for ch in t):
                code = t
                break

        # date
        date_text = None
        date_node = c.select_one("time") or c.select_one(".date, .meta time")
        date_text = text_or_none(date_node)

        # score/rating
        score = None
        score_node = c.select_one(".score, .rating, .value")
        score = text_or_none(score_node)

        # actors / categories
        actors: List[str] = []
        categories: List[str] = []

        for actor_node in c.select(".actors a, .star-name, .actor, .cast a"):
            t = text_or_none(actor_node)
            if t:
                actors.append(t)

        for cat_node in c.select(".category a, .genres a, .tag a, .meta .item a"):
            t = text_or_none(cat_node)
            if t:
                categories.append(t)

        # Deduplicate by detail_url
        if detail_url in seen:
            continue
        seen.add(detail_url)

        item = JavdbItem(
            title=title,
            code=code,
            date=date_text,
            score=score,
            actors=actors,
            categories=categories,
            cover_url=cover_url,
            detail_url=detail_url,
        )
        items.append(item)

    # Fallback: look for cards with `section.videos .grid .item`
    if not items:
        for c in soup.select("section .grid .item"):
            a = c.select_one("a[href]")
            if not a:
                continue
            href = a.get("href")
            if not href:
                continue
            detail_url = href if href.startswith("http") else _join_url(base_url, href)
            title = text_or_none(c.select_one(".title, h3, h4")) or text_or_none(a) or ""
            img = a.select_one("img") or c.select_one("img")
            cover_url = None
            if img:
                cover_url = img.get("data-src") or img.get("src")
            items.append(
                JavdbItem(
                    title=title,
                    code=None,
                    date=None,
                    score=None,
                    actors=[],
                    categories=[],
                    cover_url=cover_url,
                    detail_url=detail_url,
                )
            )

    logging.debug(f"Parsed {len(items)} items from page {base_url}")
    return items


def _join_url(base: str, href: str) -> str:
    if href.startswith("/"):
        # Extract origin
        from urllib.parse import urlsplit, urlunsplit

        sp = urlsplit(base)
        origin = urlunsplit((sp.scheme, sp.netloc, "", "", ""))
        return origin + href
    return href


def find_next_page_url(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    # Try rel=next
    a = soup.select_one("a[rel=next]")
    if a and a.get("href"):
        href = a["href"]
        return href if href.startswith("http") else _join_url(current_url, href)

    # Try text-based
    for selector in [
        "a.next, a.pagination-next, .pagination a.next",
        "a:contains('下一頁')",
        "a:contains('Next')",
    ]:
        a = soup.select_one(selector)
        if a and a.get("href"):
            href = a["href"]
            return href if href.startswith("http") else _join_url(current_url, href)

    # Fallback: try query param page increment if present
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    parsed = urlparse(current_url)
    qs = parse_qs(parsed.query)
    if "page" in qs:
        try:
            cur = int(qs["page"][0])
            qs["page"] = [str(cur + 1)]
            new_query = urlencode(qs, doseq=True)
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
        except Exception:
            pass

    return None


def save_json(items: List[JavdbItem], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([asdict(i) for i in items], f, ensure_ascii=False, indent=2)


def save_csv(items: List[JavdbItem], path: str) -> None:
    fieldnames = [
        "title",
        "title_zh",
        "code",
        "date",
        "score",
        "actors",
        "actors_zh",
        "categories",
        "categories_zh",
        "cover_url",
        "detail_url",
        "magnets",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in items:
            row = asdict(i)
            row["actors"] = ", ".join(i.actors)
            row["categories"] = ", ".join(i.categories)
            if i.actors_zh is not None:
                row["actors_zh"] = ", ".join(i.actors_zh)
            if i.categories_zh is not None:
                row["categories_zh"] = ", ".join(i.categories_zh)
            row["magnets"] = " | ".join(i.magnets) if i.magnets else ""
            writer.writerow(row)


def polite_sleep(delay_seconds: float) -> None:
    jitter = delay_seconds * 0.25
    time.sleep(max(0.0, delay_seconds + random.uniform(-jitter, jitter)))


def scrape_listing(
    start_url: str,
    max_pages: int,
    delay_seconds: float,
    session: requests.Session,
) -> List[JavdbItem]:
    all_items: List[JavdbItem] = []
    url = start_url
    pages_crawled = 0

    while url and pages_crawled < max_pages:
        logging.info(f"Crawl page {pages_crawled + 1}/{max_pages}: {url}")
        p0 = perf_counter()
        soup = get_soup(session, url)
        p1 = perf_counter()
        items = parse_listing_items(soup, base_url=url)
        p2 = perf_counter()
        logging.info(
            f"Page {pages_crawled + 1}: fetch={(p1-p0)*1000:.1f} ms, parse={(p2-p1)*1000:.1f} ms, items={len(items)}"
        )
        all_items.extend(items)
        pages_crawled += 1

        next_url = find_next_page_url(soup, current_url=url)
        if not next_url:
            logging.info("No next page found; stopping pagination")
            break
        logging.debug(f"Sleeping ~{delay_seconds:.2f}s before next page")
        polite_sleep(delay_seconds)
        url = next_url

    return all_items


def parse_detail_for_magnets(soup: BeautifulSoup) -> List[str]:
    magnets_all: List[Tuple[str, str]] = []  # (href, label_text)
    # Common places: anchor href startswith magnet:, sometimes inside tables or lists
    for a in soup.select('a[href^="magnet:"]'):
        href = a.get("href")
        if not href or not href.startswith("magnet:"):
            continue
        # Build a label context for filtering: own text + parent text snippet
        label = a.get_text(strip=True) or ""
        parent = a.find_parent()
        if parent is not None:
            parent_text = parent.get_text(" ", strip=True)
            # Cap length to avoid overly long blobs
            if parent_text and len(parent_text) < 200:
                label = f"{label} {parent_text}".strip()
        magnets_all.append((href, label))

    # Deduplicate by href while preserving order
    seen: set = set()
    deduped_all: List[Tuple[str, str]] = []
    for href, label in magnets_all:
        if href in seen:
            continue
        seen.add(href)
        deduped_all.append((href, label))

    # Prefer Chinese-subtitle variants when detectable
    # Heuristics: label contains any of these markers
    chs_markers = ["-C", "中字", "中文字幕", "CHS", "Chinese", "简中", "繁中", "zh", "cn"]
    def looks_chs(text: str) -> bool:
        t = text or ""
        lt = t.lower()
        if "-c" in lt or "chs" in lt or "chinese" in lt or " zh" in lt or " cn" in lt:
            return True
        for m in chs_markers:
            if m in t:
                return True
        return False

    preferred = [href for href, label in deduped_all if looks_chs(label)]
    if preferred:
        return preferred
    # Fallback to all if no Chinese marker found
    return [href for href, _ in deduped_all]


def fetch_details_and_magnets(
    items: List[JavdbItem],
    session: requests.Session,
    delay_seconds: float,
    show_progress: bool,
) -> None:
    total = len(items)
    for idx, it in enumerate(items, start=1):
        if not it.detail_url:
            continue
        logging.info(f"Detail {idx}/{total}: {it.detail_url}")
        t0 = perf_counter()
        try:
            soup = get_soup(session, it.detail_url)
            t1 = perf_counter()
            it.magnets = parse_detail_for_magnets(soup)
            t2 = perf_counter()
            logging.info(
                f"Detail parsed: fetch={(t1-t0)*1000:.1f} ms, parse={(t2-t1)*1000:.1f} ms, magnets={len(it.magnets)}"
            )
        except requests.RequestException as e:
            logging.warning(f"Detail fetch failed: {e}")
        if show_progress:
            pct = (idx / total) * 100.0
            sys.stderr.write(f"\r[details] {idx}/{total} ({pct:.0f}%)")
            sys.stderr.flush()
        polite_sleep(delay_seconds)
    if show_progress:
        sys.stderr.write("\n")
        sys.stderr.flush()


# ---------------- Translation Utilities ----------------

_JP_RANGES = [
    (0x3040, 0x309F),  # Hiragana
    (0x30A0, 0x30FF),  # Katakana
    (0x4E00, 0x9FFF),  # CJK Unified Ideographs (includes many Kanji)
]


def looks_japanese(text: str) -> bool:
    for ch in text:
        code = ord(ch)
        for lo, hi in _JP_RANGES:
            if lo <= code <= hi:
                return True
    return False


def translate_mymemory(session: requests.Session, text: str, source_lang: str, target_lang: str) -> Optional[str]:
    # Public, rate-limited free API. Keep requests short and cache aggressively.
    import urllib.parse as _ul

    url = (
        "https://api.mymemory.translated.net/get?" +
        _ul.urlencode({
            "q": text,
            "langpair": f"{source_lang}|{target_lang}",
        })
    )
    r = session.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        resp = data.get("responseData") or {}
        translated = resp.get("translatedText")
        if translated:
            return str(translated)
    return None


def translate_libre(session: requests.Session, text: str, source_lang: str, target_lang: str) -> Optional[str]:
    # Uses public LibreTranslate instance; subject to rate limits
    url = "https://libretranslate.com/translate"
    r = session.post(url, data={
        "q": text,
        "source": source_lang,
        "target": target_lang,
        "format": "text",
    }, timeout=25)
    r.raise_for_status()
    data = r.json()
    out = data.get("translatedText") if isinstance(data, dict) else None
    return out if out else None


def translate_google(session: requests.Session, text: str, source_lang: str, target_lang: str) -> Optional[str]:
    # Unofficial public endpoint; subject to change and rate limits
    import urllib.parse as _ul
    url = (
        "https://translate.googleapis.com/translate_a/single?" +
        _ul.urlencode({
            "client": "gtx",
            "sl": source_lang,
            "tl": target_lang,
            "dt": "t",
            "q": text,
        })
    )
    r = session.get(url, timeout=20, headers={"accept": "application/json"})
    r.raise_for_status()
    data = r.json()
    # Expected structure: [[ [translated, original, ...], ... ], ...]
    if isinstance(data, list) and data and isinstance(data[0], list):
        parts: List[str] = []
        for seg in data[0]:
            if isinstance(seg, list) and seg:
                out = seg[0]
                if isinstance(out, str):
                    parts.append(out)
        return "".join(parts) if parts else None
    return None


def translate_items(
    items: List[JavdbItem],
    provider: str,
    source_lang: str,
    target_lang: str,
    delay_seconds: float,
    show_progress: bool = False,
) -> None:
    # Build unique text pool to minimize API calls
    unique_texts: List[str] = []
    index_map: Dict[str, int] = {}

    def add_text(t: Optional[str]):
        if not t:
            return
        if not looks_japanese(t):
            return
        if t not in index_map:
            index_map[t] = len(unique_texts)
            unique_texts.append(t)

    for it in items:
        add_text(it.title)
        for a in it.actors:
            add_text(a)
        for c in it.categories:
            add_text(c)

    if not unique_texts:
        return

    mt_session = requests.Session()
    cache: Dict[str, str] = {}

    def do_translate(text: str) -> Optional[str]:
        if text in cache:
            return cache[text]
        try:
            if provider == "libre":
                out = translate_libre(mt_session, text, source_lang, target_lang)
            elif provider == "google":
                out = translate_google(mt_session, text, source_lang, target_lang)
            else:
                out = translate_mymemory(mt_session, text, source_lang, target_lang)
            if out:
                cache[text] = out
            return out
        finally:
            polite_sleep(delay_seconds)

    # Perform translations
    done = 0
    total = len(unique_texts)
    t_all0 = perf_counter()
    for t in unique_texts:
        _ = do_translate(t)
        done += 1
        if show_progress:
            pct = (done / total) * 100.0
            sys.stderr.write(f"\r[translate] {done}/{total} ({pct:.0f}%)")
            sys.stderr.flush()
        elif done % 10 == 0 or done == total:
            logging.debug(f"Translated {done}/{total} strings")
    if show_progress:
        sys.stderr.write("\n")
        sys.stderr.flush()
    logging.info(f"Translation finished in {(perf_counter()-t_all0):.2f}s")


def translate_titles_only(
    items: List[JavdbItem],
    provider: str,
    source_lang: str,
    target_lang: str,
    delay_seconds: float,
    show_progress: bool = False,
) -> None:
    # Build unique title set
    unique_titles: List[str] = []
    seen: set = set()
    for it in items:
        if it.title and looks_japanese(it.title) and it.title not in seen:
            seen.add(it.title)
            unique_titles.append(it.title)

    if not unique_titles:
        return

    mt_session = requests.Session()
    cache: Dict[str, str] = {}

    def do_translate(text: str) -> Optional[str]:
        if text in cache:
            return cache[text]
        try:
            if provider == "libre":
                out = translate_libre(mt_session, text, source_lang, target_lang)
            elif provider == "google":
                out = translate_google(mt_session, text, source_lang, target_lang)
            else:
                out = translate_mymemory(mt_session, text, source_lang, target_lang)
            if out:
                cache[text] = out
            return out
        finally:
            polite_sleep(delay_seconds)

    logging.info(f"Translating titles only: {len(unique_titles)} unique strings")
    done = 0
    total = len(unique_titles)
    for t in unique_titles:
        _ = do_translate(t)
        done += 1
        if show_progress:
            pct = (done / total) * 100.0
            sys.stderr.write(f"\r[title] {done}/{total} ({pct:.0f}%)")
            sys.stderr.flush()
        elif done % 10 == 0 or done == total:
            logging.debug(f"Translated titles {done}/{total}")
    if show_progress:
        sys.stderr.write("\n")
        sys.stderr.flush()

    for it in items:
        if it.title in cache:
            it.title_zh = cache[it.title]

    # Attach translations back to items
    for it in items:
        if it.title and it.title in cache:
            it.title_zh = cache[it.title]
        translated_actors: List[str] = []
        for a in it.actors:
            translated_actors.append(cache.get(a, a))
        translated_categories: List[str] = []
        for c in it.categories:
            translated_categories.append(cache.get(c, c))
        it.actors_zh = translated_actors if any(looks_japanese(x) for x in it.actors) else None
        it.categories_zh = translated_categories if any(looks_japanese(x) for x in it.categories) else None


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape javdb listing pages politely.")
    p.add_argument("--url", required=True, help="Listing URL to start from")
    p.add_argument("--max-pages", type=int, default=1, help="Max number of pages to crawl")
    p.add_argument("--delay", type=float, default=1.5, help="Delay seconds between requests")
    p.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    p.add_argument("--user-agent", default=None, help="Custom User-Agent string")
    p.add_argument("--cookies", default=None, help="Raw cookies string: name=value; name2=value2")
    p.add_argument("--proxy", default=None, help="Proxy URL, e.g. http://127.0.0.1:7890")
    p.add_argument("--format", choices=["json", "csv"], default="json", help="Output format")
    p.add_argument("--out", default="javdb_output.json", help="Output file path")
    # logging options
    p.add_argument("--log-level", default="INFO", help="Logging level: DEBUG, INFO, WARNING, ERROR")
    p.add_argument("--log-file", default=None, help="Write logs to file (default: stderr)")
    # translation options
    p.add_argument("--translate", action="store_true", help="Enable translation of Japanese to Chinese")
    p.add_argument("--translator", choices=["mymemory", "libre", "google"], default="google", help="Translation provider")
    p.add_argument("--source-lang", default="ja", help="Source language code (default: ja)")
    p.add_argument("--target-lang", default="zh-CN", help="Target language code (default: zh-CN)")
    p.add_argument("--mt-delay", type=float, default=1.0, help="Delay seconds between translation calls")
    p.add_argument("--progress", action="store_true", default=True, help="Show live translation progress on stderr")
    p.add_argument("--translate-default", action="store_true", default=True, help="Translate titles by default even if --translate is off")
    # details options
    p.add_argument("--details", action="store_true", default=True, help="Fetch each detail page and extract magnets")
    p.add_argument("--detail-delay", type=float, default=1.0, help="Delay seconds between detail requests")
    # search options
    p.add_argument(
        "--search",
        action="append",
        default=None,
        help="Filter by keyword(s). Can be repeated or comma-separated.",
    )
    p.add_argument("--search-to-ja", action="store_true", default=True, help="Translate search keyword to Japanese before filtering")
    p.add_argument("--search-mode", choices=["or", "and"], default="or", help="Multi-keyword match mode: or/and")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    # setup logging early
    level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    handlers: List[logging.Handler] = []
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s", handlers=handlers)
    logging.debug("Logger initialized")
    session = build_session(
        user_agent=args.user_agent,
        cookies=args.cookies,
        proxy=args.proxy,
        timeout=args.timeout,
    )

    try:
        items = scrape_listing(
            start_url=args.url,
            max_pages=args.max_pages,
            delay_seconds=args.delay,
            session=session,
        )
    except requests.HTTPError as e:
        logging.error("HTTP error: %s", e)
        return 2
    except requests.RequestException as e:
        logging.error("Request failed: %s", e)
        return 3

    # Optional search filter FIRST: translate keyword to Japanese and match against original fields
    if args.search:
        # Collect multi keywords (split by commas and multiple --search flags)
        raw_keys: List[str] = []
        for entry in (args.search or []):
            if entry is None:
                continue
            parts = [p.strip() for p in str(entry).split(',') if p.strip()]
            raw_keys.extend(parts)
        raw_keys = [k for k in raw_keys if k]
        if not raw_keys:
            logging.info("No valid search keywords after parsing; skipping filter")
        else:
            # Translate each keyword to JA if enabled
            ja_keys: List[str] = raw_keys[:]
            if args.search_to_ja:
                try:
                    mt_session = requests.Session()
                    out: List[str] = []
                    for k in raw_keys:
                        ja = translate_google(mt_session, k, args.target_lang, "ja")
                        out.append(ja if isinstance(ja, str) and ja else k)
                    ja_keys = out
                    logging.info(f"Search keywords -> JA: {list(zip(raw_keys, ja_keys))}")
                except Exception as e:
                    logging.warning(f"Search keyword translation failed, using original: {e}")
                    ja_keys = raw_keys

            raw_keys_lc = [k.lower() for k in raw_keys]
            ja_keys_lc = [k.lower() for k in ja_keys]

            def hit_any(it: JavdbItem) -> bool:
                # originals (likely JA)
                orig_fields: List[str] = [it.title]
                orig_fields.extend(it.actors)
                orig_fields.extend(it.categories)
                of_lc = [(s or '').lower() for s in orig_fields]
                # translated fallback fields
                zh_fields: List[str] = []
                if it.title_zh:
                    zh_fields.append(it.title_zh)
                if it.actors_zh:
                    zh_fields.extend(it.actors_zh)
                if it.categories_zh:
                    zh_fields.extend(it.categories_zh)
                zh_lc = [(s or '').lower() for s in zh_fields]

                def match_one(k_en_lc: str, k_ja_lc: str) -> bool:
                    if any(k_ja_lc in f for f in of_lc):
                        return True
                    if zh_lc and any(k_en_lc in f for f in zh_lc):
                        return True
                    return False

                if args.search_mode == 'and':
                    for k_en_lc, k_ja_lc in zip(raw_keys_lc, ja_keys_lc):
                        if not match_one(k_en_lc, k_ja_lc):
                            return False
                    return True
                else:
                    for k_en_lc, k_ja_lc in zip(raw_keys_lc, ja_keys_lc):
                        if match_one(k_en_lc, k_ja_lc):
                            return True
                    return False

            before = len(items)
            items = [it for it in items if hit_any(it)]
            logging.info(f"Search filter matched {len(items)}/{before} items; mode={args.search_mode}; keys={raw_keys}; ja={ja_keys}")

    # After filtering, perform translation
    # Full translation (title/actors/categories)
    if args.translate and items:
        try:
            translate_items(
                items=items,
                provider=args.translator,
                source_lang=args.source_lang,
                target_lang=args.target_lang,
                delay_seconds=args.mt_delay,
                show_progress=args.progress,
            )
        except Exception as e:
            logging.warning("Translation failed (skipping): %s", e)

    # Default title translation only (lighter weight)
    if (not args.translate) and args.translate_default and items:
        try:
            translate_titles_only(
                items=items,
                provider=args.translator,
                source_lang=args.source_lang,
                target_lang=args.target_lang,
                delay_seconds=args.mt_delay,
                show_progress=args.progress,
            )
        except Exception as e:
            logging.warning("Title translation failed (skipping): %s", e)

    # Optional detail scraping step
    if args.details and items:
        fetch_details_and_magnets(
            items=items,
            session=session,
            delay_seconds=args.detail_delay,
            show_progress=args.progress,
        )

    # Save output
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    if args.format == "json":
        if not args.out.lower().endswith(".json"):
            args.out += ".json"
        save_json(items, args.out)
    else:
        if not args.out.lower().endswith(".csv"):
            args.out += ".csv"
        save_csv(items, args.out)

    logging.info(f"Saved {len(items)} items to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


