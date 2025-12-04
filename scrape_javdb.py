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
    """
    Wrapper around session.get with small extra retry for flaky TLS issues like SSLEOFError.
    We already have urllib3 Retry on the session, but some SSL errors still bubble up.
    """
    from requests.exceptions import SSLError, ConnectionError, ReadTimeout

    max_attempts = 3
    last_err: Optional[Exception] = None

    for attempt in range(1, max_attempts + 1):
        t0 = perf_counter()
        logging.debug(f"HTTP GET ({attempt}/{max_attempts}) -> {url}")
        try:
            resp = session.get(url)
            dt = (perf_counter() - t0) * 1000
            logging.debug(
                f"HTTP {resp.status_code} <- {url} ({len(resp.content)} bytes in {dt:.1f} ms)"
            )
            resp.raise_for_status()
            encoding = resp.apparent_encoding or detect_encoding(resp.content)
            html = resp.content.decode(encoding, errors="ignore")
            return BeautifulSoup(html, "lxml")
        except (SSLError, ConnectionError, ReadTimeout) as e:
            last_err = e
            logging.warning(
                f"HTTP TLS/connection error on attempt {attempt}/{max_attempts} for {url}: {e}"
            )
            if attempt < max_attempts:
                # short backoff; main politeness delay happens outside
                backoff = 0.5 * attempt
                logging.debug(f"Retrying {url} after {backoff:.1f}s due to TLS/connection error")
                time.sleep(backoff)
            else:
                logging.error(
                    f"Giving up on {url} after {max_attempts} attempts due to TLS/connection errors"
                )
        except Exception as e:
            # Other errors propagate immediately
            logging.error(f"Unexpected error fetching {url}: {e}", exc_info=True)
            raise

    # Should not reach here without last_err, but guard just in case
    raise last_err or RuntimeError(f"Failed to fetch {url} due to repeated TLS/connection errors")


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


def download_cover(session: requests.Session, cover_url: str, local_path: str) -> bool:
    """Download a cover image with retries"""
    try:
        resp = session.get(cover_url, timeout=15, stream=True)
        resp.raise_for_status()
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        logging.warning(f"Failed to download cover {cover_url}: {e}")
        return False


def download_covers(items: List[JavdbItem], session: requests.Session, covers_dir: str) -> None:
    """Download all cover images"""
    if not items:
        return
    
    os.makedirs(covers_dir, exist_ok=True)
    downloaded = 0
    total = sum(1 for item in items if item.cover_url)
    
    if total == 0:
        logging.info("No cover images to download")
        return
    
    logging.info(f"Downloading {total} cover images to {covers_dir}/")
    
    for i, item in enumerate(items, 1):
        if not item.cover_url:
            continue
        
        # Generate filename: Chinese title + first magnet hash
        title_part = item.title_zh or item.title or "unknown"
        # Sanitize title for filename
        safe_title = "".join(c for c in title_part if c.isalnum() or c in (' ', '-', '_', '(', ')')).strip()
        safe_title = safe_title[:30]  # Limit title length
        
        # Use just the title for filename
        filename = f"{safe_title}.jpg"
        
        # Ensure unique filename
        base_path = os.path.join(covers_dir, filename)
        counter = 1
        local_path = base_path
        while os.path.exists(local_path):
            name, ext = os.path.splitext(base_path)
            local_path = f"{name}_{counter}{ext}"
            counter += 1
        
        if download_cover(session, item.cover_url, local_path):
            downloaded += 1
            # Update cover_url to local path for display
            item.cover_url = local_path
        
        if i % 5 == 0 or i == total:
            logging.info(f"Downloaded {downloaded}/{i} covers")
    
    logging.info(f"Downloaded {downloaded}/{total} cover images")


def display_results(items: List[JavdbItem]) -> None:
    """Display results in a terminal-friendly format"""
    logging.info("\n" + "="*80)
    logging.info(f"Found {len(items)} items:")
    logging.info("="*80)
    
    for i, item in enumerate(items, 1):
        logging.info(f"\n[{i}] {item.title_zh or item.title}")
        if item.code:
            logging.info(f"    Code: {item.code}")
        if item.actors:
            logging.info(f"    Actors: {', '.join(item.actors[:3])}{'...' if len(item.actors) > 3 else ''}")
        if item.categories:
            logging.info(f"    Categories: {', '.join(item.categories[:3])}{'...' if len(item.categories) > 3 else ''}")
        if item.cover_url:
            if item.cover_url.startswith('http'):
                logging.info(f"    Cover: {item.cover_url}")
            else:
                logging.info(f"    Cover: {item.cover_url} (local)")
        if item.magnets:
            logging.info(f"    Magnets ({len(item.magnets)}):")
            for j, magnet in enumerate(item.magnets, 1):
                logging.info(f"      {j}. {magnet}")
        if item.detail_url:
            logging.info(f"    Detail: {item.detail_url}")
        logging.info("-" * 80)


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
    all_links = soup.select('a[href^="magnet:"]')
    logging.debug(f"Found {len(all_links)} magnet links in page")
    
    if len(all_links) == 0:
        # Try to find any links and log a sample
        any_links = soup.select('a[href]')
        sample_links = [(a.get('href', '')[:50], a.get_text(strip=True)[:30]) for a in any_links[:5]]
        logging.warning(f"No magnet links found. Sample of other links: {sample_links}")
        return []
    
    for a in all_links:
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
        logging.debug(f"Found magnet: {label[:50]} -> {href[:50]}...")

    logging.debug(f"Collected {len(magnets_all)} raw magnet links")

    # Deduplicate by href while preserving order
    seen: set = set()
    deduped_all: List[Tuple[str, str]] = []
    for href, label in magnets_all:
        if href in seen:
            logging.debug(f"Dropped duplicate magnet: {href[:50]}...")
            continue
        seen.add(href)
        deduped_all.append((href, label))

    logging.debug(f"After deduplication: {len(deduped_all)} unique magnets")

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
        return True

    preferred = [href for href, label in deduped_all if looks_chs(label)]
    if preferred:
        logging.debug(f"Using {len(preferred)} preferred magnets with Chinese markers")
        return preferred
    # Fallback to all if no Chinese marker found
    result = [href for href, _ in deduped_all]
    logging.debug(f"No Chinese markers found, returning all {len(result)} magnets")
    return result


def fetch_details_and_magnets(
    items: List[JavdbItem],
    session: requests.Session,
    delay_seconds: float,
    show_progress: bool,
) -> None:
    import random
    total = len(items)
    ANTI_SCRAPE_MAX = 30
    anti_spam_keywords = [
        "验证码", "verify", "認証", "認證", "anti-bot", "robot check", "登录", "登錄", "forbidden", "access denied", "人机验证", "too many requests", "429", "频率", "频繁", "過於頻繁", "captcha"
    ]
    limit = min(total, ANTI_SCRAPE_MAX)
    if total > ANTI_SCRAPE_MAX:
        logging.warning(f"Only processing first {ANTI_SCRAPE_MAX} items for details/magnets to avoid anti-scraping trigger. Total items: {total} (rest will stay basic)")
    for idx, it in enumerate(items[:limit], start=1):
        if not it.detail_url:
            continue
        logging.info(f"Detail {idx}/{total}: {it.detail_url}")
        t0 = perf_counter()
        success = False
        for retry_count in range(3):
            try:
                soup = get_soup(session, it.detail_url)
                html_text = str(soup)
                # 反爬虫检测
                anti_sign = [k for k in anti_spam_keywords if k.lower() in html_text.lower()]
                if anti_sign:
                    logging.error(f"Anti-crawler detected! Keywords: {anti_sign} in {it.detail_url}")
                    sleep_time = delay_seconds * (2 ** retry_count) + random.uniform(0, 1)
                    logging.warning(f"Sleeping {sleep_time:.1f}s due to anti-crawler for {it.detail_url}")
                    time.sleep(sleep_time)
                    continue

                t1 = perf_counter()
                it.magnets = parse_detail_for_magnets(soup)
                t2 = perf_counter()
                if len(it.magnets) == 0:
                    logging.warning(f"No magnets found for {it.code or 'item'}: {it.detail_url}")
                logging.info(
                    f"Detail parsed: fetch={(t1-t0)*1000:.1f} ms, parse={(t2-t1)*1000:.1f} ms, magnets={len(it.magnets)}"
                )
                success = True
                break
            except requests.RequestException as e:
                logging.warning(f"Detail fetch failed for {it.code or 'item'} (try {retry_count+1}/3): {e} (URL: {it.detail_url})")
            except Exception as e:
                logging.error(f"Unexpected error parsing magnets for {it.code or 'item'} (try {retry_count+1}/3): {e}", exc_info=True)
            backoff = delay_seconds * (2 ** retry_count) + random.uniform(0, 1)
            logging.info(f"Retrying in {backoff:.1f}s...")
            time.sleep(backoff)
        if not success:
            logging.error(f"[AbortDetail] Failed to parse detail after 3 attempts: {it.detail_url}")
        final_delay = delay_seconds + random.uniform(0, 1)
        if show_progress:
            pct = (idx / total) * 100.0
            sys.stderr.write(f"\r[details] {idx}/{total} ({pct:.0f}%)")
            sys.stderr.flush()
        time.sleep(final_delay)
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
    proxy: Optional[str] = None,
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
    # Reuse main HTTP proxy for MT APIs if provided
    if proxy:
        mt_session.proxies.update({"http": proxy, "https": proxy})
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
    proxy: Optional[str] = None,
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
    if proxy:
        mt_session.proxies.update({"http": proxy, "https": proxy})
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
    p.add_argument("--start-page", type=int, default=1, help="Page number to start scraping from (default: 1)")
    p.add_argument("--delay", type=float, default=1.5, help="Delay seconds between requests")
    p.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    p.add_argument("--user-agent", default=None, help="Custom User-Agent string")
    p.add_argument("--cookies", default=None, help="Raw cookies string: name=value; name2=value2")
    p.add_argument("--proxy", default=None, help="Proxy URL, e.g. http://127.0.0.1:7890")
    p.add_argument("--out", default="javdb_output", help="Output file base name (will create both .json and .csv)")
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
    p.add_argument("--show", action="store_true", help="Display results in terminal (title_zh, cover, magnets)")
    p.add_argument("--download-covers", action="store_true", help="Download cover images locally")
    p.add_argument("--covers-dir", default="covers", help="Directory to save cover images (default: covers)")
    p.add_argument("--auto-folder", action="store_true", default=True, help="Auto-create folder with keywords+timestamp for organized results")
    return p.parse_args(argv)


def create_auto_folder(args) -> str:
    """Create auto folder with keywords and timestamp"""
    if not args.auto_folder:
        return ""
    
    # Get keywords for folder name
    keywords = []
    if args.search:
        for entry in (args.search or []):
            if entry is None:
                continue
            parts = [p.strip() for p in str(entry).split(',') if p.strip()]
            keywords.extend(parts)
    
    # Create folder name
    if keywords:
        # Sanitize keywords for folder name
        safe_keywords = []
        for k in keywords[:3]:  # Limit to first 3 keywords
            safe_k = "".join(c for c in k if c.isalnum() or c in (' ', '-', '_')).strip()
            safe_keywords.append(safe_k)
        folder_name = "_".join(safe_keywords)
    else:
        folder_name = "javdb_results"
    
    # Add timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    auto_folder = f"{folder_name}_{timestamp}"
    
    os.makedirs(auto_folder, exist_ok=True)
    return auto_folder


def _set_page_in_url(url: str, page: int) -> str:
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs['page'] = [str(page)]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    
    # Create auto folder if enabled
    auto_folder = create_auto_folder(args)
    if auto_folder:
        # Update paths to use auto folder
        if not args.log_file:
            args.log_file = os.path.join(auto_folder, "scrape.log")
        if not args.out.startswith("/") and not args.out.startswith("\\"):
            args.out = os.path.join(auto_folder, args.out)
        if args.download_covers and args.covers_dir == "covers":
            args.covers_dir = os.path.join(auto_folder, "covers")
    
    # setup logging early
    level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    handlers: List[logging.Handler] = []
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s", handlers=handlers)
    logging.debug("Logger initialized")
    
    if auto_folder:
        logging.info(f"Auto folder created: {auto_folder}")
    session = build_session(
        user_agent=args.user_agent,
        cookies=args.cookies,
        proxy=args.proxy,
        timeout=args.timeout,
    )

    # 动态构造起始url
    start_url = args.url
    if args.start_page and args.start_page > 1:
        if 'page=' in args.url:
            start_url = _set_page_in_url(args.url, args.start_page)
        else:
            if '?' in args.url:
                start_url = args.url + f'&page={args.start_page}'
            else:
                start_url = args.url + f'?page={args.start_page}'
        logging.info(f"Start page set to {args.start_page}: {start_url}")

    # 调用分页方法
    try:
        items = scrape_listing(
            start_url=start_url,
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
                    if args.proxy:
                        mt_session.proxies.update({"http": args.proxy, "https": args.proxy})
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
                proxy=args.proxy,
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
                proxy=args.proxy,
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

    # Optional cover download
    if args.download_covers and items:
        download_covers(items, session, args.covers_dir)

    # Save output in both formats
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    
    # Create both JSON and CSV files
    json_path = args.out if args.out.lower().endswith(".json") else f"{args.out}.json"
    csv_path = args.out if args.out.lower().endswith(".csv") else f"{args.out}.csv"
    
    save_json(items, json_path)
    save_csv(items, csv_path)
    
    logging.info(f"Saved {len(items)} items to {json_path} and {csv_path}")
    
    # Optional terminal display
    if args.show and items:
        display_results(items)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


