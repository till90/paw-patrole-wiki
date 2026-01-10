#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_dataset.py mit manuellem Term-Mapper
"""


import datetime as dt
import json
import os
import re
import time
from typing import Any, Dict, List, Optional

# ============================================================
# 0) HEADER – HIER ALLES EINSTELLEN
# ============================================================

APP_DIR = os.path.dirname(os.path.abspath(__file__))
IN_JSON_PATH = os.path.join(APP_DIR, "out_pawpatrol_characters", "characters.json")
OUT_JSON_PATH = os.path.join(APP_DIR, "out_pawpatrol_characters", "characters_de.json")

GCP_PROJECT_ID = "data-tales-481512"
GCP_LOCATION = "global"
TARGET_LANG = "de"
SOURCE_LANG = "en"

CREDENTIALS_JSON_PATH = os.path.join(APP_DIR, "..", "data-tales-481512-4af38e5c8dca.json")
LABELS_ONLY = False
CACHE_PATH = OUT_JSON_PATH + ".translate_cache_de.json"

BATCH_MAX_CHARS = 8000
BATCH_MAX_ITEMS = 50
MAX_RETRIES = 5
BASE_BACKOFF_S = 0.8

# ============================================================
# 1) Manueller Mapper & Label Mapping
# ============================================================

# NEU: Begriffe, die hart ersetzt werden, bevor die API gefragt wird.
# Funktioniert für ganze Strings oder Teile davon.
MANUAL_TERM_MAPPER = MANUAL_TERM_MAPPER = {
    # --- Charaktere (Menschen & Tiere) ---
    "Mayor Goodway": "Bürgermeisterin Gutherz",
    "Mayor Humdinger": "Bürgermeister Besserwisser",
    "Chickaletta": "Henrietta",
    "Captain Turbot": "Käpt'n Tollpatsch",

    "Francois Turbot": "Francois Tollpatsch",
    "Mr. Porter": "Herr Pfeffer",
    "Alex Porter": "Alex Pfeffer",
    "Farmer Yumi": "Bäuerin Yumi",
    "Farmer Al": "Bauer Al",
    "Sid Swashbuckle": "Sid der Säbelrassler",
    "The Copycat": "Der Nachahmer",
    "Harold Humdinger": "Harold Besserwisser",
    "Princess of Barkingburg": "Prinzessin von Barkingburg",
    "The Duke of Flappington": "Der Herzog von Flappington",
    "Santa Claus": "Weihnachtsmann",
    "Tilly Turbot": "Tilly Tollpatsch",
    "Cap'n Turbot": "Käpt'n Tollpatsch",
    "Captain Turbot": "Käpt'n Tollpatsch",
    "Horatio Turbot": "Horatio Tollpatsch",
    "Francois Turbot": "Francois Tollpatsch",
    "Dr. Turbot": "Dr. Tollpatsch",
    "Tammy Turbot": "Tammy Tollpatsch",
    "Taylor Turbot": "Taylor Tollpatsch",

    # --- Die Bösewichte (Villains) ---
    "Mayor Humdinger": "Bürgermeister Besserwisser",
    "Kitten Catastrophe Crew": "Katzastrophe-Crew",
    "Cat Chase": "Miau-Chase",
    "Cat Marshall": "Miau-Marshall",
    "Cat Skye": "Miau-Skye",
    "Cat Rocky": "Miau-Rocky",
    "Cat Rubble": "Miau-Rubble",
    "Cat Zuma": "Miau-Zuma",
    "Meow-Meow": "Miau-Miau",
    
    "The Copycat": "Der Nachahmer",
    "Ladybird": "Ladybird",
    "The Cheetah": "Die Gepardin",
    "The Duke of Flappington": "Der Herzog von Flappington",
    "Sid Swashbuckle": "Sid der Säbelrassler",
    "Arrby": "Arrby",
    "Claw": "Claw",
    "The Ruff-Ruff Pack": "Die Racker-Racker-Bande",
    "Hubcap": "Hubcap",
    "Dwayne": "Dwayne",
    "Gasket": "Gasket",
    "Moby": "Moby",

# --- Wichtige Orte & Fraktionen ---
    "Foggy Bottom": "Nebelburg",
    "Barkingburg": "Barkingburg",
    "Adventure City": "Abenteuerstadt",
    "Dino Wilds": "Dino-Wildnis",
    "Dragon Mountain": "Drachenberg",

    # --- Fahrzeuge & Hauptquartiere (Vehicles & Bases) ---
    "The Lookout": "Der Aussichtsturm",
    "Air Patroller": "Air Patrouiller",
    "Sea Patroller": "Meeres-Patrouiller",
    "PAW Patroller": "PAW Patrouiller",
    "Mission Cruiser": "Mission-Cruiser",
    "Mighty Jet": "Mighty Jet",
    "Dino Patroller": "Dino-Patrouiller",
    "Whale Patroller": "Wal-Patrouiller",
    "Sub Patroller": "U-Boot-Patrouiller",
    "PAW Mover": "PAW-Mover",
    "The Flounder": "Die Flunder",
    "Mobile Pit Stop": "Mobiler Boxenstopp",

    # --- Begriffe & Ausrüstung ---
    "pup": "Welpe",
    "pups": "Welpen",
    "Pup Pad": "Welpen-Pad",
    "Air Patroller": "Air Patrouiller",
    "Sea Patroller": "Meeres-Patrouiller",
    "Mighty Pups": "Mighty Pups",
    "Ultimate Rescue": "Ultimativer Einsatz",
    "Mission PAW": "Mission Pfote",
    "Dino Rescue": "Dino-Rettung",
    "badge": "Abzeichen",
    "uniform": "Uniform",
    "mother": "Mutter",
    
    # --- Die Namen der Welpen bleiben im Deutschen meist gleich, 
    # aber falls du Rollen übersetzen willst: ---
    "firedog": "Feuerwehrhund",
    "police dog": "Polizeihund",
    "recycling pup": "Recycling-Welpe",

    # --- Medien & Berufe ---
    "camerawoman": "Kamerafrau",
    "camerawomen": "Kamerafrauen",
    "cameraman": "Kameramann",
    "reporter": "Reporter",
    "news crew": "Nachrichtenteam",
    "Adventure Bay News": "Nachrichten aus der Abenteuerbucht",
    
    # Oft wird die Kamerafrau im Englischen auch einfach nur 
    # deskriptiv genannt, falls sie keinen Namen hat:
    "The camerawoman": "Die Kamerafrau",
}

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

def apply_manual_mapping(text: str) -> str:
    """Ersetzt Begriffe basierend auf MANUAL_TERM_MAPPER."""
    if not text:
        return text
    for eng, deu in MANUAL_TERM_MAPPER.items():
        # Nutzt Regex für Wortgrenzen-Sicherheit oder einfaches replace
        # Hier einfaches Case-Sensitive Replace für Teilstrings:
        if eng in text:
            text = text.replace(eng, deu)
    return text

def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def clean_text(s: Any) -> str:
    if s is None: return ""
    s = str(s)
    s = CONTROL_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    return s

def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    return obj

def save_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_cache(path: str) -> Dict[str, str]:
    if not os.path.exists(path): return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except: return {}

def save_cache(path: str, cache: Dict[str, str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def map_label(label: Any) -> str:
    k = clean_text(label)
    return LABEL_DE.get(k, k)

def is_nonempty_str(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""

def ensure_credentials_env() -> None:
    if LABELS_ONLY: return
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS") and CREDENTIALS_JSON_PATH:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(CREDENTIALS_JSON_PATH)

# ============================================================
# 3) Translator Class
# ============================================================

class GCloudTranslator:
    def __init__(self, project_id, location, target_lang, source_lang, cache):
        from google.cloud import translate_v3 as translate
        self.client = translate.TranslationServiceClient()
        self.parent = f"projects/{project_id}/locations/{location}"
        self.target_lang = target_lang
        self.source_lang = source_lang
        self.cache = cache if cache is not None else {}

    def _translate_request(self, contents: List[str]) -> List[str]:
        req = {
            "parent": self.parent,
            "contents": contents,
            "mime_type": "text/plain",
            "target_language_code": self.target_lang,
            "source_language_code": self.source_lang,
        }
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.client.translate_text(request=req)
                return [tr.translated_text for tr in resp.translations]
            except Exception as e:
                time.sleep(min(10.0, BASE_BACKOFF_S * (2 ** (attempt - 1))))
        return contents # Fallback auf Original bei totalem Failure

    def translate_many(self, texts: List[str]) -> Dict[str, str]:
        uniq = list(dict.fromkeys([clean_text(t) for t in texts if clean_text(t)]))
        
        # NEU: Erst manuelles Mapping prüfen
        for t in uniq:
            if t not in self.cache:
                mapped = apply_manual_mapping(t)
                if mapped != t:
                    self.cache[t] = mapped

        todo = [t for t in uniq if t not in self.cache]
        if todo:
            for i in range(0, len(todo), BATCH_MAX_ITEMS):
                batch = todo[i : i + BATCH_MAX_ITEMS]
                translated = self._translate_request(batch)
                for src, dst in zip(batch, translated):
                    # Auch auf das Ergebnis der API nochmal den manuellen Mapper loslassen
                    self.cache[src] = apply_manual_mapping(clean_text(dst))

        return {t: self.cache[t] for t in uniq if t in self.cache}

# ============================================================
# 4) Transform Logic
# ============================================================

def collect_texts_for_translation(dataset: Dict[str, Any]) -> List[str]:
    texts = []
    for ch in dataset.get("characters", []):
        if not isinstance(ch, dict): continue
        # NEU: Namen zur Übersetzungsliste hinzufügen
        if is_nonempty_str(ch.get("name")): texts.append(ch["name"])
        if is_nonempty_str(ch.get("link_text_from_list")): texts.append(ch["link_text_from_list"])
        
        if is_nonempty_str(ch.get("summary")): texts.append(ch["summary"])
        for v in ch.get("profile_flat", {}).values():
            if is_nonempty_str(v): texts.append(v)
        for it in ch.get("profile", []):
            if is_nonempty_str(it.get("value")): texts.append(it["value"])
        for g in ch.get("profile_groups", []):
            for it in g.get("fields", []):
                if is_nonempty_str(it.get("value")): texts.append(it["value"])
    return texts

def transform_dataset(src: Dict[str, Any], translator: Optional[GCloudTranslator], translate_texts: bool) -> Dict[str, Any]:
    out = json.loads(json.dumps(src))
    
    lookup = {}
    if translate_texts and translator:
        all_texts = collect_texts_for_translation(out)
        lookup = translator.translate_many(all_texts)

    def tr(val: Any) -> Any:
        s = clean_text(val)
        if not s: return val
        if not translate_texts:
            return apply_manual_mapping(s)
        return lookup.get(s, apply_manual_mapping(s))

    for ch in out.get("characters", []):
        # --- NEU: Hier werden jetzt auch die Namen übersetzt ---
        if is_nonempty_str(ch.get("name")):
            ch["name"] = tr(ch["name"])
        
        if is_nonempty_str(ch.get("link_text_from_list")):
            ch["link_text_from_list"] = tr(ch["link_text_from_list"])
        # -------------------------------------------------------

        if is_nonempty_str(ch.get("summary")): 
            ch["summary"] = tr(ch["summary"])
        
        if isinstance(ch.get("profile_flat"), dict):
            ch["profile_flat"] = {map_label(k): tr(v) for k, v in ch["profile_flat"].items()}

        for it in ch.get("profile", []):
            it["label"] = map_label(it.get("label"))
            it["value"] = tr(it.get("value"))

        for g in ch.get("profile_groups", []):
            g["group"] = tr(g.get("group"))
            for it in g.get("fields", []):
                it["label"] = map_label(it.get("label"))
                it["value"] = tr(it.get("value"))

    return out

def main():
    if not os.path.exists(IN_JSON_PATH): return 1
    src = load_json(IN_JSON_PATH)
    ensure_credentials_env()
    cache = load_cache(CACHE_PATH)

    translator = None
    if not LABELS_ONLY:
        translator = GCloudTranslator(GCP_PROJECT_ID, GCP_LOCATION, TARGET_LANG, SOURCE_LANG, cache)

    out = transform_dataset(src, translator, not LABELS_ONLY)
    save_json(OUT_JSON_PATH, out)
    if translator: save_cache(CACHE_PATH, translator.cache)
    print(f"Fertig! Datei gespeichert unter: {OUT_JSON_PATH}")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())