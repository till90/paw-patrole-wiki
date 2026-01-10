#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PAW Patrol Wiki (Fandom) – Character Crawler
- Reads all character links from: https://pawpatrol.fandom.com/wiki/List_of_characters
- Fetches each character page (best-effort) and extracts a "Steckbrief" from the Portable Infobox
- Downloads the primary infobox image (if available)
- Stores everything in ONE JSON file in a structure suited for a later "grid + modal/details" project page

IMPORTANT (Lizenz/Reuse):
- Text on the PAW Patrol Wiki is CC BY-SA 3.0 (Unported) "unless otherwise specified".
- Images on Fandom are frequently NOT CC BY-SA; the file page / extmetadata may indicate non-free or other restrictions.
  This script therefore stores per-image license metadata when available, and marks unknown/non-free cases explicitly.

Usage:
  python crawl_pawpatrol_fandom_characters.py
  python crawl_pawpatrol_fandom_characters.py --out out_paw --max 50 --sleep 0.8
  python crawl_pawpatrol_fandom_characters.py --resume

Output:
  <out>/
    characters.json
    images/
      <slug>.<ext>

Deps:
  pip install requests beautifulsoup4
"""



import argparse
import datetime as dt
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

# -----------------------------
# CONFIG (defaults)
# -----------------------------

WIKI_BASE = "https://pawpatrol.fandom.com"
API_URL = f"{WIKI_BASE}/api.php"

LIST_PAGE_TITLE = "List_of_characters"
LIST_PAGE_URL = f"{WIKI_BASE}/wiki/{LIST_PAGE_TITLE}"

COPYRIGHTS_URL = f"{WIKI_BASE}/wiki/PAW_Patrol_Wiki:Copyrights"
DEFAULT_TEXT_LICENSE = "CC BY-SA 3.0 (Unported) — unless otherwise specified"

DEFAULT_USER_AGENT = os.getenv(
    "USER_AGENT",
    "data-tales.dev crawler (contact: info@data-tales.dev) - respectful rate limited",
)

HTTP_CONNECT_TIMEOUT = float(os.getenv("HTTP_CONNECT_TIMEOUT", "5"))
HTTP_READ_TIMEOUT = float(os.getenv("HTTP_READ_TIMEOUT", "20"))

# Strict link filtering: accept only /wiki/<Title> with no query string and no namespaces (':')
WIKI_PATH_RE = re.compile(r"^/wiki/([^?#]+)$")

# Slugging for local filenames/ids
SLUG_KEEP_RE = re.compile(r"[^a-z0-9\-]+")


@dataclass
class ImageLicenseInfo:
    file_title: Optional[str] = None
    file_page_url: Optional[str] = None
    original_url: Optional[str] = None
    description_url: Optional[str] = None
    mime: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    extmetadata: Dict[str, Any] = None
    non_free: Optional[bool] = None
    license_short: Optional[str] = None
    license_url: Optional[str] = None
    usage_terms: Optional[str] = None
    attribution: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_title": self.file_title,
            "file_page_url": self.file_page_url,
            "original_url": self.original_url,
            "description_url": self.description_url,
            "mime": self.mime,
            "width": self.width,
            "height": self.height,
            "license_short": self.license_short,
            "license_url": self.license_url,
            "usage_terms": self.usage_terms,
            "non_free": self.non_free,
            "attribution": self.attribution,
            "extmetadata": self.extmetadata or {},
        }


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def slugify(title: str) -> str:
    s = (title or "").strip().lower()
    s = s.replace("&", "and")
    s = s.replace("’", "'")
    s = re.sub(r"[\"“”]", "", s)
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^a-z0-9\-]", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    s = SLUG_KEEP_RE.sub("", s)
    return s or "item"


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\[[0-9A-Za-z]+\]", "", s).strip()  # remove reference markers like [1]
    return s


def parse_boolish(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        vv = v.strip().lower()
        if vv in ("true", "yes", "1"):
            return True
        if vv in ("false", "no", "0"):
            return False
    return None


class FandomClient:
    def __init__(self, sleep_s: float = 0.7, max_retries: int = 4) -> None:
        self.sleep_s = sleep_s
        self.max_retries = max_retries
        self.s = requests.Session()
        self.s.headers.update(
            {
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
            }
        )

    def _sleep(self) -> None:
        if self.sleep_s > 0:
            time.sleep(self.sleep_s)

    def api_get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Always request JSON
        params = dict(params)
        params.setdefault("format", "json")
        params.setdefault("formatversion", "2")

        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.s.get(API_URL, params=params, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
                if r.status_code in (429, 503, 502):
                    # Backoff
                    wait = min(8.0, self.sleep_s * (2 ** (attempt - 1)) + 0.2)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                data = r.json()
                self._sleep()
                return data
            except Exception as e:
                last_err = e
                wait = min(8.0, self.sleep_s * (2 ** (attempt - 1)) + 0.2)
                time.sleep(wait)

        raise RuntimeError(f"API request failed after retries: {last_err}")

    def parse_html(self, page_title: str) -> Tuple[str, str]:
        """
        Returns (normalized_title, html)
        """
        data = self.api_get(
            {
                "action": "parse",
                "page": page_title,
                "prop": "text",
                "redirects": "1",
            }
        )
        if "error" in data:
            info = (data.get("error") or {}).get("info") or "unknown API error"
            raise RuntimeError(f"parse failed for {page_title}: {info}")

        parse = data.get("parse") or {}
        norm_title = parse.get("title") or page_title
        html = (parse.get("text") or "").strip()
        if not html:
            raise RuntimeError(f"empty HTML for {page_title}")
        return norm_title, html

    def page_meta(self, page_title: str) -> Dict[str, Any]:
        """
        Query: revision ids/timestamp + pageimage (if any)
        """
        data = self.api_get(
            {
                "action": "query",
                "titles": page_title,
                "prop": "revisions|pageimages",
                "rvprop": "ids|timestamp",
                "pithumbsize": 1200,
                "pilimit": 1,
                "redirects": "1",
            }
        )
        pages = ((data.get("query") or {}).get("pages")) or []
        if not pages:
            return {}
        page = pages[0] or {}
        rev = None
        revs = page.get("revisions") or []
        if revs:
            rev = revs[0]
        thumb = page.get("thumbnail") or {}
        original = page.get("original") or {}
        return {
            "pageid": page.get("pageid"),
            "title": page.get("title") or page_title,
            "revision_id": (rev or {}).get("revid"),
            "revision_timestamp": (rev or {}).get("timestamp"),
            "pageimage": page.get("pageimage"),
            "thumbnail_url": thumb.get("source"),
            "original_image_url": original.get("source"),
        }

    def image_info(self, file_title: str) -> ImageLicenseInfo:
        """
        Fetch imageinfo (url + extmetadata when available).
        file_title must be like "File:XYZ.png"
        """
        data = self.api_get(
            {
                "action": "query",
                "titles": file_title,
                "prop": "imageinfo",
                "iiprop": "url|size|mime|extmetadata",
                "iilimit": 1,
                "redirects": "1",
            }
        )

        pages = ((data.get("query") or {}).get("pages")) or []
        if not pages:
            return ImageLicenseInfo(file_title=file_title, extmetadata={})

        page = pages[0] or {}
        ii = (page.get("imageinfo") or [])
        if not ii:
            return ImageLicenseInfo(file_title=file_title, extmetadata={})

        info = ii[0] or {}
        ext = info.get("extmetadata") or {}

        # extmetadata values are objects: { "value": "...", ... } in many MW installs; normalize to plain strings
        ext_flat: Dict[str, Any] = {}
        for k, v in ext.items():
            if isinstance(v, dict) and "value" in v:
                ext_flat[k] = v.get("value")
            else:
                ext_flat[k] = v

        # best-effort license fields
        lic_short = ext_flat.get("LicenseShortName") or ext_flat.get("License")
        lic_url = ext_flat.get("LicenseUrl")
        usage_terms = ext_flat.get("UsageTerms")
        non_free = parse_boolish(ext_flat.get("NonFree"))  # often "true"/"false" (string)

        attribution = ext_flat.get("Attribution") or ext_flat.get("Credit") or ext_flat.get("Artist")

        return ImageLicenseInfo(
            file_title=page.get("title") or file_title,
            file_page_url=f"{WIKI_BASE}/wiki/{quote((page.get('title') or file_title).replace(' ', '_'))}",
            original_url=info.get("url"),
            description_url=info.get("descriptionurl"),
            mime=info.get("mime"),
            width=info.get("width"),
            height=info.get("height"),
            extmetadata=ext_flat,
            non_free=non_free,
            license_short=clean_text(str(lic_short)) if lic_short else None,
            license_url=clean_text(str(lic_url)) if lic_url else None,
            usage_terms=clean_text(str(usage_terms)) if usage_terms else None,
            attribution=clean_text(str(attribution)) if attribution else None,
        )

    def download(self, url: str) -> Tuple[bytes, str]:
        """
        Returns (content, content_type)
        """
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.s.get(url, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT), stream=True)
                if r.status_code in (429, 503, 502):
                    wait = min(8.0, self.sleep_s * (2 ** (attempt - 1)) + 0.2)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                ct = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                content = r.content
                self._sleep()
                return content, ct
            except Exception as e:
                last_err = e
                wait = min(8.0, self.sleep_s * (2 ** (attempt - 1)) + 0.2)
                time.sleep(wait)
        raise RuntimeError(f"download failed: {url} ({last_err})")


def infer_ext(url: str, content_type: str) -> str:
    ct = (content_type or "").lower()
    if ct in ("image/png",):
        return "png"
    if ct in ("image/jpeg", "image/jpg"):
        return "jpg"
    if ct in ("image/webp",):
        return "webp"
    if ct in ("image/gif",):
        return "gif"

    # fallback from URL
    path = urlsplit(url).path.lower()
    for ext in ("png", "jpg", "jpeg", "webp", "gif"):
        if path.endswith("." + ext):
            return "jpg" if ext == "jpeg" else ext
    return "bin"


def extract_character_links_from_list_html(html: str) -> List[Tuple[str, str]]:
    """
    Returns list of (link_text, absolute_url) from the List_of_characters page.
    Strict filtering: /wiki/<Title> only, no query/fragment, no namespaces.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Fandom parse HTML typically includes mw-parser-output
    root = soup.select_one("div.mw-parser-output") or soup

    links: List[Tuple[str, str]] = []
    seen = set()

    for a in root.select("a[href]"):
        href = a.get("href") or ""
        text = clean_text(a.get_text(" ", strip=True))
        if not text:
            continue

        # Absolute wiki links sometimes appear; normalize
        if href.startswith("http://") or href.startswith("https://"):
            if not href.startswith(WIKI_BASE + "/wiki/"):
                continue
            href_path = urlsplit(href).path
        else:
            href_path = urlsplit(href).path

        m = WIKI_PATH_RE.match(href_path)
        if not m:
            continue

        # Exclude namespaces (Special:, Category:, File:, etc.) — keep only content pages
        title_part = unquote(m.group(1))
        if ":" in title_part:
            continue

        if title_part.replace(" ", "_") == LIST_PAGE_TITLE:
            continue

        abs_url = urljoin(WIKI_BASE, href_path)

        key = abs_url.lower()
        if key in seen:
            continue
        seen.add(key)
        links.append((text, abs_url))

    return links


def title_from_wiki_url(url: str) -> Optional[str]:
    """
    https://pawpatrol.fandom.com/wiki/Chase -> "Chase"
    https://.../wiki/PAW_Patrol_(team) -> "PAW_Patrol_(team)"
    """
    try:
        u = urlsplit(url)
        if not u.path.startswith("/wiki/"):
            return None
        t = u.path.split("/wiki/", 1)[1]
        if not t:
            return None
        return unquote(t)
    except Exception:
        return None


def parse_portable_infobox(html: str) -> Dict[str, Any]:
    """
    Extracts a "steckbrief" structure from <aside class="portable-infobox"> if present.
    Returns:
      {
        "title": "...",
        "image": { "img_url": "...", "file_href": "/wiki/File:..." },
        "fields": [ {"label": "...", "value": "..."} ],
        "groups": [ {"group": "...", "fields": [..]} ]
      }
    """
    soup = BeautifulSoup(html, "html.parser")
    infobox = soup.select_one("aside.portable-infobox")
    if not infobox:
        return {"title": None, "image": None, "fields": [], "groups": []}

    # Title inside infobox (if any)
    ib_title = None
    title_el = infobox.select_one(".pi-title")
    if title_el:
        ib_title = clean_text(title_el.get_text(" ", strip=True))

    # Image: prefer file page link if present
    img_url = None
    file_href = None

    # common patterns: figure.pi-image or a.image inside
    fig = infobox.select_one("figure.pi-item.pi-image") or infobox.select_one("figure.pi-item")
    if fig:
        a_img = fig.select_one("a[href]")
        if a_img:
            href = a_img.get("href") or ""
            if href.startswith("/wiki/File:") or "/wiki/File:" in href:
                file_href = href if href.startswith("/wiki/") else urlsplit(href).path
        img = fig.select_one("img")
        if img:
            img_url = img.get("data-src") or img.get("src")

    # Fields without groups
    fields: List[Dict[str, str]] = []
    for item in infobox.select(".pi-item.pi-data"):
        label_el = item.select_one(".pi-data-label")
        value_el = item.select_one(".pi-data-value")
        if not label_el or not value_el:
            continue
        label = clean_text(label_el.get_text(" ", strip=True))
        value = clean_text(value_el.get_text(" ", strip=True))
        if label and value:
            fields.append({"label": label, "value": value})

    # Grouped fields
    groups: List[Dict[str, Any]] = []
    for grp in infobox.select("section.pi-item.pi-group"):
        header = grp.select_one(".pi-header")
        gname = clean_text(header.get_text(" ", strip=True)) if header else ""
        gfields: List[Dict[str, str]] = []
        for item in grp.select(".pi-item.pi-data"):
            label_el = item.select_one(".pi-data-label")
            value_el = item.select_one(".pi-data-value")
            if not label_el or not value_el:
                continue
            label = clean_text(label_el.get_text(" ", strip=True))
            value = clean_text(value_el.get_text(" ", strip=True))
            if label and value:
                gfields.append({"label": label, "value": value})
        if gname or gfields:
            groups.append({"group": gname or None, "fields": gfields})

    return {
        "title": ib_title,
        "image": {"img_url": img_url, "file_href": file_href} if (img_url or file_href) else None,
        "fields": fields,
        "groups": groups,
    }


def extract_lead_summary(html: str, max_chars: int = 420) -> Optional[str]:
    """
    Best-effort first paragraph summary (kept short for project cards/modals).
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("div.mw-parser-output") or soup

    for p in root.select("p"):
        txt = clean_text(p.get_text(" ", strip=True))
        if not txt:
            continue
        # skip typical non-content boilerplate if present
        if txt.lower().startswith(("stub", "this article", "this page")):
            continue
        if len(txt) < 40:
            continue
        if len(txt) > max_chars:
            txt = txt[: max_chars - 1].rstrip() + "…"
        return txt
    return None


def file_title_from_file_href(file_href: str) -> Optional[str]:
    if not file_href:
        return None
    # /wiki/File:Chase.png  -> File:Chase.png
    path = urlsplit(file_href).path
    if "/wiki/" in path:
        t = path.split("/wiki/", 1)[1]
    else:
        t = path.lstrip("/")
    t = unquote(t)
    if t.startswith("File:"):
        return t
    return None


def build_attribution_text(page_title: str, page_url: str, retrieved_at: str, revision_id: Optional[int]) -> str:
    rev = f", revision {revision_id}" if revision_id else ""
    return f'Text source: PAW Patrol Wiki (Fandom) — "{page_title}" ({page_url}) — retrieved {retrieved_at}{rev} — {DEFAULT_TEXT_LICENSE}'


def build_image_attribution(img_info: ImageLicenseInfo, retrieved_at: str) -> str:
    # If extmetadata provides a better attribution string, use it, else fall back to file page.
    bits = []
    if img_info.attribution:
        bits.append(img_info.attribution)
    if img_info.file_page_url:
        bits.append(img_info.file_page_url)
    bits.append(f"retrieved {retrieved_at}")
    if img_info.license_short:
        bits.append(img_info.license_short)
    elif img_info.usage_terms:
        bits.append(img_info.usage_terms)
    return " | ".join([b for b in bits if b])


def load_existing(out_json_path: str) -> Dict[str, Any]:
    if not os.path.exists(out_json_path):
        return {}
    with open(out_json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="out_pawpatrol_characters", help="Output directory")
    ap.add_argument("--max", type=int, default=0, help="Limit number of characters (0 = no limit)")
    ap.add_argument("--sleep", type=float, default=0.7, help="Sleep between requests (seconds)")
    ap.add_argument("--resume", action="store_true", help="Resume from existing JSON (skip already crawled)")
    ap.add_argument("--lead-max", type=int, default=420, help="Max chars for lead summary")
    args = ap.parse_args()

    out_dir = args.out
    images_dir = os.path.join(out_dir, "images")
    out_json = os.path.join(out_dir, "characters.json")

    ensure_dir(out_dir)
    ensure_dir(images_dir)

    client = FandomClient(sleep_s=args.sleep)

    retrieved_at = utc_now_iso()

    existing = load_existing(out_json) if args.resume else {}
    existing_ids = set()
    if existing and isinstance(existing.get("characters"), list):
        for ch in existing["characters"]:
            if isinstance(ch, dict) and ch.get("id"):
                existing_ids.add(ch["id"])

    # 1) Parse list page
    list_norm_title, list_html = client.parse_html(LIST_PAGE_TITLE)
    links = extract_character_links_from_list_html(list_html)

    # Optional max
    if args.max and args.max > 0:
        links = links[: args.max]

    # 2) Crawl each character
    dataset = existing if (args.resume and existing) else {
        "meta": {
            "dataset": "pawpatrol-characters",
            "source_site": "PAW Patrol Wiki - Fandom",
            "source_list_title": list_norm_title,
            "source_list_url": LIST_PAGE_URL,
            "content_license_default": DEFAULT_TEXT_LICENSE,
            "content_license_url": COPYRIGHTS_URL,
            "retrieved_at": retrieved_at,
            "generator": {
                "script": os.path.basename(__file__),
            },
            "notes": [
                "Text is CC BY-SA 3.0 (Unported) unless otherwise specified. Images may have different/non-free licenses; verify per file.",
                "This dataset is structured for a later grid page: characters[*].image.local_path + characters[*].profile + characters[*].summary",
            ],
        },
        "characters": [],
    }

    # If resuming, refresh meta timestamp
    dataset["meta"]["retrieved_at"] = retrieved_at

    seen_titles = set()
    for idx, (link_text, abs_url) in enumerate(links, start=1):
        page_title = title_from_wiki_url(abs_url) or link_text
        page_title = page_title.replace(" ", "_")  # API accepts underscores
        if page_title.lower() in seen_titles:
            continue
        seen_titles.add(page_title.lower())

        # We'll use the normalized page title from parse/meta for stable naming
        try:
            meta = client.page_meta(page_title)
            norm_title = meta.get("title") or page_title.replace("_", " ")
            char_id = slugify(norm_title)

            if args.resume and char_id in existing_ids:
                print(f"[SKIP] {idx}/{len(links)} {norm_title} (already in JSON)")
                continue

            print(f"[CRAWL] {idx}/{len(links)} {norm_title}")

            _, page_html = client.parse_html(page_title)
            infobox = parse_portable_infobox(page_html)
            summary = extract_lead_summary(page_html, max_chars=args.lead_max)

            page_url = f"{WIKI_BASE}/wiki/{quote(norm_title.replace(' ', '_'))}"

            # --- image handling ---
            image_block = infobox.get("image") or {}
            file_title = None

            # Prefer file_href from infobox
            if image_block.get("file_href"):
                file_title = file_title_from_file_href(image_block.get("file_href"))

            # Fallback: use pageimage from meta
            if not file_title and meta.get("pageimage"):
                # meta.pageimage is filename without "File:" in many MW setups
                file_title = f"File:{meta['pageimage']}"

            img_info = None
            local_img_rel = None
            local_img_abs = None
            img_sha256 = None

            if file_title:
                img_info = client.image_info(file_title)
                img_url = img_info.original_url
                if img_url:
                    content, ct = client.download(img_url)
                    ext = infer_ext(img_url, ct)
                    local_img_rel = f"images/{char_id}.{ext}"
                    local_img_abs = os.path.join(out_dir, local_img_rel)

                    with open(local_img_abs, "wb") as f:
                        f.write(content)
                    img_sha256 = sha256_hex(content)

                    # enrich attribution
                    img_info.attribution = build_image_attribution(img_info, retrieved_at)
            else:
                # last-resort: use infobox img_url (likely a thumb); still download
                img_url = image_block.get("img_url")
                if img_url:
                    content, ct = client.download(img_url)
                    ext = infer_ext(img_url, ct)
                    local_img_rel = f"images/{char_id}.{ext}"
                    local_img_abs = os.path.join(out_dir, local_img_rel)
                    with open(local_img_abs, "wb") as f:
                        f.write(content)
                    img_sha256 = sha256_hex(content)

            # flat profile dict for easy UI rendering
            flat_profile: Dict[str, str] = {}
            for kv in infobox.get("fields") or []:
                if kv.get("label") and kv.get("value"):
                    flat_profile[kv["label"]] = kv["value"]
            for grp in infobox.get("groups") or []:
                for kv in grp.get("fields") or []:
                    if kv.get("label") and kv.get("value") and kv["label"] not in flat_profile:
                        flat_profile[kv["label"]] = kv["value"]

            # small "tags" for filtering later (best-effort from common infobox keys)
            tag_keys = ("Species", "Breed", "Team", "Affiliation", "Occupation", "Status")
            tags = []
            for k in tag_keys:
                v = flat_profile.get(k)
                if v:
                    tags.append(f"{k}: {v}")
            tags = tags[:8]

            char_obj = {
                "id": char_id,
                "name": norm_title,
                "link_text_from_list": link_text,
                "tags": tags,
                "source": {
                    "page_title": norm_title,
                    "page_url": page_url,
                    "list_url": LIST_PAGE_URL,
                    "text_license_default": DEFAULT_TEXT_LICENSE,
                    "text_license_url": COPYRIGHTS_URL,
                    "retrieved_at": retrieved_at,
                    "revision_id": meta.get("revision_id"),
                    "revision_timestamp": meta.get("revision_timestamp"),
                    "attribution": build_attribution_text(
                        page_title=norm_title,
                        page_url=page_url,
                        retrieved_at=retrieved_at,
                        revision_id=meta.get("revision_id"),
                    ),
                },
                "summary": summary,
                "profile": infobox.get("fields") or [],
                "profile_groups": infobox.get("groups") or [],
                "profile_flat": flat_profile,
                "image": {
                    "local_path": local_img_rel,
                    "sha256": img_sha256,
                    "info": img_info.to_dict() if img_info else None,
                },
            }

            dataset["characters"].append(char_obj)

            # checkpoint every 10
            if len(dataset["characters"]) % 10 == 0:
                save_json(out_json, dataset)

        except Exception as e:
            print(f"[WARN] Failed for {abs_url}: {e}")
            # keep going

    # final save
    save_json(out_json, dataset)

    print(f"[DONE] Wrote: {out_json} (characters={len(dataset['characters'])})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
