#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_dataset.py
Wandelt:
  out_pawpatrol_characters/characters.json  ->  out_pawpatrol_characters/characters_de.json

Konfiguration erfolgt ausschließlich im HEADER unten (keine CLI-Args).

Voraussetzungen:
  pip install google-cloud-translate
  Service-Account-JSON vorhanden (für Google Cloud Translate v3 / ADC)

Hinweis:
- Dieses Script übersetzt:
  - character["summary"]
  - profile_flat Werte (Labels werden per Mapping LABEL_DE auf Deutsch gesetzt)
  - profile / profile_groups (Labels gemappt, Werte übersetzt)
- Es nutzt eine persistente Cache-Datei, damit Wiederholungen günstig sind.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from typing import Any, Dict, List, Optional

# ============================================================
# 0) HEADER – HIER ALLES EINSTELLEN
# ============================================================

# Pfade: relativ zu diesem Script (empfohlen)
APP_DIR = os.path.dirname(os.path.abspath(__file__))

IN_JSON_PATH = os.path.join(APP_DIR, "out_pawpatrol_characters", "characters.json")
OUT_JSON_PATH = os.path.join(APP_DIR, "out_pawpatrol_characters", "characters_de.json")

# GCP Translate v3
GCP_PROJECT_ID = "data-tales-481512"     # <-- anpassen
GCP_LOCATION = "global"                  # "global" ist üblich
TARGET_LANG = "de"
SOURCE_LANG = "en"                       # "" oder None = auto-detect; "en" ist oft stabiler

# Credentials: Service-Account-JSON (hier liegt die Datei im Überordner)
CREDENTIALS_JSON_PATH = os.path.join(APP_DIR, "..", "data-tales-481512-4af38e5c8dca.json")

# Wenn True: nur Labels mappen, keine API-Übersetzung (kein GCP nötig)
LABELS_ONLY = False

# Übersetzungs-Cache (persistiert, spart API Calls)
CACHE_PATH = OUT_JSON_PATH + ".translate_cache_de.json"

# Batch/Retry Tuning
BATCH_MAX_CHARS = 8000
BATCH_MAX_ITEMS = 50
MAX_RETRIES = 5
BASE_BACKOFF_S = 0.8

# ============================================================
# 1) Label Mapping (wie gewünscht)
# ============================================================

LABEL_DE = {
    "Species": "Spezies",
    "Gender": "Geschlecht",
    "Relatives": "Beziehungen",
    "Age": "Alter",
    "Nicknames": "Spitznamen",
    "Occupation": "Rolle / Beruf",
    "First appearance": "Erster Auftritt",
    "Likes": "Mag",
    "Dislikes": "Mag nicht",
    "Voice (US/Canada):": "Stimme (US/Kanada)",
    "Voice (UK):": "Stimme (UK)",
}

# ============================================================
# 2) Helpers
# ============================================================

WS_RE = re.compile(r"\s+")
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def clean_text(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    s = CONTROL_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    return s


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError("Input JSON root must be an object.")
    return obj


def save_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_cache(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def save_cache(path: str, cache: Dict[str, str]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def map_label(label: Any) -> str:
    k = clean_text(label)
    return LABEL_DE.get(k, k)


def is_nonempty_str(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def ensure_credentials_env() -> None:
    """
    Setzt GOOGLE_APPLICATION_CREDENTIALS aus HEADER, falls nicht schon gesetzt.
    """
    if LABELS_ONLY:
        return
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    if CREDENTIALS_JSON_PATH:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(CREDENTIALS_JSON_PATH)


# ============================================================
# 3) Google Cloud Translate v3 Client
# ============================================================

class GCloudTranslator:
    def __init__(
        self,
        project_id: str,
        location: str = "global",
        target_lang: str = "de",
        source_lang: Optional[str] = None,
        cache: Optional[Dict[str, str]] = None,
    ) -> None:
        try:
            from google.cloud import translate_v3 as translate  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "google-cloud-translate ist nicht installiert. Installiere: pip install google-cloud-translate"
            ) from e

        self.translate = translate
        self.client = translate.TranslationServiceClient()
        self.parent = f"projects/{project_id}/locations/{location}"
        self.target_lang = target_lang
        self.source_lang = (source_lang or "").strip() or None
        self.cache = cache if cache is not None else {}

    def _translate_request(self, contents: List[str]) -> List[str]:
        req: Dict[str, Any] = {
            "parent": self.parent,
            "contents": contents,
            "mime_type": "text/plain",
            "target_language_code": self.target_lang,
        }
        if self.source_lang:
            req["source_language_code"] = self.source_lang

        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.client.translate_text(request=req)
                out = [tr.translated_text for tr in resp.translations]
                if len(out) != len(contents):
                    raise RuntimeError("Translate API returned unexpected result length.")
                return out
            except Exception as e:
                last_err = e
                backoff = BASE_BACKOFF_S * (2 ** (attempt - 1))
                time.sleep(min(10.0, backoff))
        raise RuntimeError(f"Translate API failed after retries: {last_err}")

    def translate_many(self, texts: List[str]) -> Dict[str, str]:
        # Deduplicate (preserve order)
        uniq: List[str] = []
        seen = set()
        for t in texts:
            t = clean_text(t)
            if not t:
                continue
            if t in seen:
                continue
            seen.add(t)
            uniq.append(t)

        todo = [t for t in uniq if t not in self.cache]
        if todo:
            i = 0
            while i < len(todo):
                batch: List[str] = []
                total_chars = 0
                while i < len(todo) and len(batch) < BATCH_MAX_ITEMS:
                    t = todo[i]
                    if batch and (total_chars + len(t) > BATCH_MAX_CHARS):
                        break
                    batch.append(t)
                    total_chars += len(t)
                    i += 1

                if not batch and i < len(todo):
                    batch = [todo[i]]
                    i += 1

                translated = self._translate_request(batch)
                for src, dst in zip(batch, translated):
                    self.cache[src] = clean_text(dst)

        return {t: self.cache[t] for t in uniq if t in self.cache}

    def translate_one(self, text: Any) -> str:
        t = clean_text(text)
        if not t:
            return ""
        if t in self.cache:
            return self.cache[t]
        m = self.translate_many([t])
        return m.get(t, t)


# ============================================================
# 4) Transform
# ============================================================

def collect_texts_for_translation(dataset: Dict[str, Any]) -> List[str]:
    texts: List[str] = []
    chars = dataset.get("characters")
    if not isinstance(chars, list):
        return texts

    for ch in chars:
        if not isinstance(ch, dict):
            continue

        if is_nonempty_str(ch.get("summary")):
            texts.append(ch["summary"])

        pf = ch.get("profile_flat")
        if isinstance(pf, dict):
            for v in pf.values():
                if is_nonempty_str(v):
                    texts.append(v)

        prof = ch.get("profile")
        if isinstance(prof, list):
            for it in prof:
                if isinstance(it, dict):
                    if is_nonempty_str(it.get("label")):
                        texts.append(it["label"])
                    if is_nonempty_str(it.get("value")):
                        texts.append(it["value"])

        pgs = ch.get("profile_groups")
        if isinstance(pgs, list):
            for g in pgs:
                if not isinstance(g, dict):
                    continue
                if is_nonempty_str(g.get("group")):
                    texts.append(g["group"])
                fields = g.get("fields")
                if isinstance(fields, list):
                    for it in fields:
                        if isinstance(it, dict):
                            if is_nonempty_str(it.get("label")):
                                texts.append(it["label"])
                            if is_nonempty_str(it.get("value")):
                                texts.append(it["value"])

    return texts


def transform_dataset(
    src: Dict[str, Any],
    translator: Optional[GCloudTranslator],
    translate_texts: bool,
) -> Dict[str, Any]:
    out = json.loads(json.dumps(src))  # deep copy

    meta = out.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        out["meta"] = meta

    meta.setdefault("transforms", [])
    if isinstance(meta["transforms"], list):
        meta["transforms"].append(
            {
                "type": "translate_to_de",
                "at": utc_now_iso(),
                "label_mapping": True,
                "value_translation": bool(translate_texts),
                "translator": "google-cloud-translate (v3)" if translate_texts else None,
                "target_language": "de",
            }
        )

    # Build translation lookup (batch) to minimize calls
    lookup: Dict[str, str] = {}
    if translate_texts and translator is not None:
        all_texts = collect_texts_for_translation(out)
        lookup = translator.translate_many(all_texts)

    def tr(val: Any) -> Any:
        if not translate_texts or translator is None:
            return val
        s = clean_text(val)
        if not s:
            return val
        return lookup.get(s) or translator.translate_one(s)

    chars = out.get("characters")
    if not isinstance(chars, list):
        return out

    for ch in chars:
        if not isinstance(ch, dict):
            continue

        if is_nonempty_str(ch.get("summary")):
            ch["summary"] = tr(ch["summary"])

        pf = ch.get("profile_flat")
        if isinstance(pf, dict):
            new_pf: Dict[str, Any] = {}
            for k, v in pf.items():
                k_de = map_label(k)
                new_pf[k_de] = tr(v) if is_nonempty_str(v) else v
            ch["profile_flat"] = new_pf

        prof = ch.get("profile")
        if isinstance(prof, list):
            for it in prof:
                if not isinstance(it, dict):
                    continue
                if is_nonempty_str(it.get("label")):
                    mapped = map_label(it["label"])
                    it["label"] = tr(mapped) if (translate_texts and translator and mapped == clean_text(it["label"])) else mapped
                if is_nonempty_str(it.get("value")):
                    it["value"] = tr(it["value"])

        pgs = ch.get("profile_groups")
        if isinstance(pgs, list):
            for g in pgs:
                if not isinstance(g, dict):
                    continue
                if is_nonempty_str(g.get("group")):
                    g["group"] = tr(g["group"])
                fields = g.get("fields")
                if isinstance(fields, list):
                    for it in fields:
                        if not isinstance(it, dict):
                            continue
                        if is_nonempty_str(it.get("label")):
                            mapped = map_label(it["label"])
                            it["label"] = tr(mapped) if (translate_texts and translator and mapped == clean_text(it["label"])) else mapped
                        if is_nonempty_str(it.get("value")):
                            it["value"] = tr(it["value"])

    return out


# ============================================================
# 5) Main
# ============================================================

def main() -> int:
    if not os.path.exists(IN_JSON_PATH):
        raise SystemExit(f"Input nicht gefunden: {IN_JSON_PATH}")

    src = load_json(IN_JSON_PATH)

    ensure_credentials_env()

    translate_texts = not LABELS_ONLY
    translator: Optional[GCloudTranslator] = None
    cache = load_cache(CACHE_PATH)

    if translate_texts:
        cred_env = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        if not cred_env or not os.path.exists(cred_env):
            raise SystemExit(
                "Credentials fehlen oder nicht gefunden.\n"
                f"- GOOGLE_APPLICATION_CREDENTIALS = {cred_env!r}\n"
                f"- Prüfe CREDENTIALS_JSON_PATH im HEADER: {os.path.abspath(CREDENTIALS_JSON_PATH)}"
            )
        if not GCP_PROJECT_ID or "YOUR_" in GCP_PROJECT_ID:
            raise SystemExit("Bitte im HEADER GCP_PROJECT_ID korrekt setzen (z. B. 'data-tales-481512').")

        translator = GCloudTranslator(
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
            target_lang=TARGET_LANG,
            source_lang=SOURCE_LANG if SOURCE_LANG else None,
            cache=cache,
        )

    out = transform_dataset(src, translator=translator, translate_texts=translate_texts)

    save_json(OUT_JSON_PATH, out)
    if translator is not None:
        save_cache(CACHE_PATH, translator.cache)

    print(f"[OK] Wrote: {OUT_JSON_PATH}")
    if translator is not None:
        print(f"[OK] Cache: {CACHE_PATH} (entries={len(translator.cache)})")
    else:
        print("[OK] LABELS_ONLY=True (keine API-Calls)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
