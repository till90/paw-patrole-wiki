#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, render_template_string, request, send_file

# ------------------------------------------------------------
# 0) INPUT-DATEN (NAV / META)
# ------------------------------------------------------------

LANDING_URL = "https://data-tales.dev/"
COOKBOOK_URL = "https://data-tales.dev/cookbook/"

# Services: aktuell nur PLZ + dieser Service
SERVICES = [
    ("PLZ → Koordinaten", "https://plz.data-tales.dev/"),
    ("Paw Patrole Wiki", "/"),
    ("Paw Patrole Quiz", "https://paw-quiz.data-tales.dev/"),
]

SERVICE_META = {
    "service_name_slug": "paw",
    "page_title": "PAW Patrol – Charaktere",
    "page_h1": "PAW Patrol Charaktere",
    "page_subtitle": "Galerie mit Steckbriefen aus dem lokalen Datenset (inkl. Quellenhinweis).",
}

# ------------------------------------------------------------
# 1) DATA PATHS (ENV overridable)
# ------------------------------------------------------------

DEFAULT_DATA_JSON_PATH = "out_pawpatrol_characters/characters_de.json"
DEFAULT_DATA_BASE_DIR = "out_pawpatrol_characters"

DATA_JSON_PATH = os.getenv("DATA_JSON_PATH", DEFAULT_DATA_JSON_PATH)
DATA_BASE_DIR = os.getenv("DATA_BASE_DIR", DEFAULT_DATA_BASE_DIR)

# ------------------------------------------------------------
# 2) VALIDATION / SECURITY
# ------------------------------------------------------------

ID_RE = re.compile(r"^[a-z0-9-]{1,80}$")
# Restrict media to known folder subtree
MEDIA_ALLOWED_PREFIX = "images/"
# Cache images publicly for a week by default (Cloud Run CDN / browser)
MEDIA_MAX_AGE_SECONDS = int(os.getenv("MEDIA_MAX_AGE_SECONDS", str(7 * 24 * 3600)))


def _is_truthy_profile_flat(v: Any) -> bool:
    return isinstance(v, dict) and any(str(k).strip() and str(val).strip() for k, val in v.items())


def _safe_realpath(base_dir: str, rel_path: str) -> Optional[str]:
    """
    Prevent path traversal: only allow paths within base_dir.
    """
    base_real = os.path.realpath(base_dir)
    candidate = os.path.realpath(os.path.join(base_real, rel_path))
    if not candidate.startswith(base_real + os.sep) and candidate != base_real:
        return None
    return candidate


def _clean_services(services: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    out = []
    for name, url in services:
        if not name or not url:
            continue
        if "<" in name or ">" in name or "<" in url or ">" in url:
            # ignore placeholder entries
            continue
        out.append((name, url))
    # If > 6, show first 6 + "Mehr…"
    return out


# ------------------------------------------------------------
# 3) LOAD + CACHE DATASET
# ------------------------------------------------------------

def _load_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def load_dataset() -> Dict[str, Any]:
    """
    Load once per process (Cloud Run instance) and keep in memory.
    """
    if not os.path.exists(DATA_JSON_PATH):
        raise FileNotFoundError(
            f"Datenset nicht gefunden: {DATA_JSON_PATH}. "
            f"Stelle sicher, dass der Ordner 'out_pawpatrol_characters/' mit deployt wird."
        )

    raw = _load_json_file(DATA_JSON_PATH)
    if not isinstance(raw, dict):
        raise ValueError("Ungültiges JSON-Format: Root muss ein Objekt sein.")

    chars = raw.get("characters")
    if not isinstance(chars, list):
        raise ValueError("Ungültiges JSON-Format: 'characters' muss eine Liste sein.")

    filtered = []
    by_id = {}

    for ch in chars:
        if not isinstance(ch, dict):
            continue

        cid = ch.get("id")
        name = ch.get("name")
        profile_flat = ch.get("profile_flat")

        if not isinstance(cid, str) or not ID_RE.match(cid):
            continue
        if not isinstance(name, str) or not name.strip():
            continue
        if not _is_truthy_profile_flat(profile_flat):
            continue

        image_rel = None
        img = ch.get("image") or {}
        if isinstance(img, dict):
            lp = img.get("local_path")
            if isinstance(lp, str) and lp.strip():
                image_rel = lp.strip()

        source = ch.get("source") or {}
        src_url = source.get("page_url") if isinstance(source, dict) else None
        src_attr = source.get("attribution") if isinstance(source, dict) else None

        # Build a compact, UI/API-friendly representation
        obj = {
            "id": cid,
            "name": name.strip(),
            "image_local_path": image_rel,  # e.g. images/chase.jpg
            "profile_flat": dict(profile_flat),
            "source_page_url": src_url if isinstance(src_url, str) else None,
            "source_attribution": src_attr if isinstance(src_attr, str) else None,
        }

        filtered.append(obj)
        by_id[cid] = obj

    # Sort for stable UI
    filtered.sort(key=lambda x: x["name"].lower())

    meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
    return {"meta": meta, "characters": filtered, "by_id": by_id}


# ------------------------------------------------------------
# 4) FLASK APP
# ------------------------------------------------------------

app = Flask(__name__)

TEMPLATE = r"""
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="theme-color" content="#0b0f19" />
  <title>{{ meta.page_title }}</title>

  <style>
  :root{
    --bg: #0b0f19;
    --bg2:#0f172a;
    --card:#111a2e;
    --text:#e6eaf2;
    --muted:#a8b3cf;
    --border: rgba(255,255,255,.10);
    --shadow: 0 18px 60px rgba(0,0,0,.35);
    --primary:#6ea8fe;
    --primary2:#8bd4ff;
    --focus: rgba(110,168,254,.45);

    --radius: 18px;
    --container: 1100px;
    --gap: 18px;

    --font: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
  }

  [data-theme="light"]{
    --bg:#f6f7fb;
    --bg2:#ffffff;
    --card:#ffffff;
    --text:#111827;
    --muted:#4b5563;
    --border: rgba(17,24,39,.12);
    --shadow: 0 18px 60px rgba(17,24,39,.10);
    --primary:#2563eb;
    --primary2:#0ea5e9;
    --focus: rgba(37,99,235,.25);
  }

  *{box-sizing:border-box}
  html,body{height:100%}
  body{
    margin:0;
    font-family:var(--font);
    background: radial-gradient(1200px 800px at 20% -10%, rgba(110,168,254,.25), transparent 55%),
                radial-gradient(1000px 700px at 110% 10%, rgba(139,212,255,.20), transparent 55%),
                linear-gradient(180deg, var(--bg), var(--bg2));
    color:var(--text);
  }

  .container{
    max-width:var(--container);
    margin:0 auto;
    padding:0 18px;
  }

  .skip-link{
    position:absolute; left:-999px; top:10px;
    background:var(--card); color:var(--text);
    padding:10px 12px; border-radius:10px;
    border:1px solid var(--border);
  }
  .skip-link:focus{left:10px; outline:2px solid var(--focus)}

  .site-header{
    position:sticky; top:0; z-index:20;
    backdrop-filter: blur(10px);
    background: rgba(10, 14, 24, .55);
    border-bottom:1px solid var(--border);
  }
  [data-theme="light"] .site-header{ background: rgba(246,247,251,.75); }

  .header-inner{
    display:flex; align-items:center; justify-content:space-between;
    padding:14px 0;
    gap:14px;
  }
  .brand{display:flex; align-items:center; gap:10px; text-decoration:none; color:var(--text); font-weight:700}
  .brand-mark{
    width:14px; height:14px; border-radius:6px;
    background: linear-gradient(135deg, var(--primary), var(--primary2));
    box-shadow: 0 10px 25px rgba(110,168,254,.25);
  }
  .nav{display:flex; gap:16px; flex-wrap:wrap}
  .nav a{color:var(--muted); text-decoration:none; font-weight:600}
  .nav a:hover{color:var(--text)}
  .header-actions{display:flex; gap:10px; align-items:center}
  .header-note{
    display:flex;
    align-items:center;
    gap:8px;
    padding:8px 10px;
    border-radius:12px;
    border:1px solid var(--border);
    background: rgba(255,255,255,.04);
    color: var(--muted);
    font-weight: 750;
    font-size: 12px;
    line-height: 1;
    white-space: nowrap;
  }

  [data-theme="light"] .header-note{
    background: rgba(17,24,39,.03);
  }

  .header-note__label{
    letter-spacing: .06em;
    text-transform: uppercase;
    font-weight: 900;
    color: var(--muted);
  }

  .header-note__mail{
    color: var(--text);
    text-decoration: none;
    font-weight: 850;
  }

  .header-note__mail:hover{
    text-decoration: underline;
  }

  /* Mobile: Label ausblenden, nur Mail zeigen */
  @media (max-width: 720px){
    .header-note__label{ display:none; }
  }
  .btn{
    display:inline-flex; align-items:center; justify-content:center;
    gap:8px;
    padding:10px 14px;
    border-radius:12px;
    border:1px solid var(--border);
    text-decoration:none;
    font-weight:700;
    color:var(--text);
    background: transparent;
    cursor:pointer;
    white-space:nowrap;
  }
  .btn:focus{outline:2px solid var(--focus); outline-offset:2px}
  .btn-primary{
    border-color: transparent;
    background: linear-gradient(135deg, var(--primary), var(--primary2));
    color: #0b0f19;
  }
  [data-theme="light"] .btn-primary{ color:#ffffff; }
  .btn-ghost{ background: transparent; }
  .btn:hover{transform: translateY(-1px)}
  .btn:active{transform:none}

  .section{padding:42px 0}

  .kicker{
    margin:0 0 10px;
    display:inline-block;
    font-weight:800;
    letter-spacing:.08em;
    text-transform:uppercase;
    color:var(--muted);
    font-size:12px;
  }
  h1{margin:0 0 12px; font-size:42px; line-height:1.1}
  @media (max-width: 520px){ h1{font-size:34px} }
  .lead{margin:0 0 18px; color:var(--muted); font-size:16px; line-height:1.6}
  .muted{color:var(--muted); line-height:1.6; margin:0}

  .grid{
    display:grid;
    grid-template-columns: repeat(3, 1fr);
    gap: var(--gap);
  }
  @media (max-width: 980px){ .grid{grid-template-columns: repeat(2, 1fr)} }
  @media (max-width: 640px){ .grid{grid-template-columns: 1fr} }

  .card{
    border:1px solid var(--border);
    border-radius: var(--radius);
    background: rgba(255,255,255,.04);
    padding:16px;
    box-shadow: var(--shadow);
    transition: transform .12s ease, border-color .12s ease;
  }
  [data-theme="light"] .card{ background: rgba(255,255,255,.92); }
  .card:hover{ transform: translateY(-2px); border-color: rgba(110,168,254,.35); }

  /* --- Character Gallery additions (minimal) --- */
  .char-card{ padding:0; overflow:hidden; }
  .char-toggle{
    width:100%;
    padding:16px;
    border:0;
    background: transparent;
    color: inherit;
    text-align:left;
    cursor:pointer;
    display:block;
  }
  .char-toggle:focus{ outline:2px solid var(--focus); outline-offset:-2px; }
  .char-title{
    font-weight:900;
    font-size:16px;
    margin:0 0 10px;
  }

  /* uniform thumbs */
  .char-thumb{
    border:1px solid var(--border);
    border-radius: 14px;
    overflow:hidden;
    background: rgba(255,255,255,.03);
    aspect-ratio: 1 / 1;
    width:100%;
  }
  [data-theme="light"] .char-thumb{ background: rgba(17,24,39,.02); }
  .char-thumb img{
    width:100%;
    height:100%;
    object-fit: contain;
    display:block;
  }
  .char-thumb.placeholder{
    display:flex;
    align-items:center;
    justify-content:center;
    color:var(--muted);
    font-weight:800;
    letter-spacing:.03em;
  }

  .char-details{
    border-top:1px solid var(--border);
    padding: 0 16px;
    max-height: 0px;
    overflow: hidden;
    transition: max-height .18s ease;
  }
  .char-card.is-open .char-details{
    padding: 14px 16px 16px;
  }

  .kv{
    display:grid;
    grid-template-columns: 1fr 1.4fr;
    gap:10px 14px;
    margin: 0;
  }
  @media (max-width: 520px){
    .kv{ grid-template-columns: 1fr; }
  }
  .kv dt{
    color: var(--muted);
    font-weight: 850;
  }
  .kv dd{
    margin:0;
    font-weight: 700;
    overflow-wrap:anywhere;
  }

  .attr{
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 13px;
    line-height: 1.55;
  }
  .attr a{
    color: var(--text);
    text-decoration:none;
    border-bottom: 1px solid transparent;
    font-weight: 850;
  }
  .attr a:hover{ border-bottom-color: var(--text); }

  .site-footer{
    border-top:1px solid var(--border);
    padding:18px 0;
  }
  .footer-inner{display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap}

  .sr-only{
    position:absolute; width:1px; height:1px; padding:0; margin:-1px;
    overflow:hidden; clip:rect(0,0,0,0); border:0;
  }
  </style>
</head>

<body>
  <a class="skip-link" href="#main">Zum Inhalt springen</a>

   <header class="site-header">
    <div class="container header-inner">
      <a class="brand" href="{{ landing_url }}" aria-label="Zur Landing Page">
        <span class="brand-mark" aria-hidden="true"></span>
        <span class="brand-text">data-tales.dev</span>
      </a>

      <div class="nav-dropdown" data-dropdown>
          <button class="btn btn-ghost nav-dropbtn"
                  type="button"
                  aria-haspopup="true"
                  aria-expanded="false"
                  aria-controls="servicesMenu">
            Dienste <span class="nav-caret" aria-hidden="true">▾</span>
          </button>

          <div id="servicesMenu" class="card nav-menu" role="menu" hidden>
            <a role="menuitem" href="https://flybi-demo.data-tales.dev/">Flybi Dashboard Demo</a>
            <a role="menuitem" href="https://wms-wfs-sources.data-tales.dev/">WMS/WFS Server Viewer</a>
            <a role="menuitem" href="https://tree-locator.data-tales.dev/">Tree Locator</a>
            <a role="menuitem" href="https://plz.data-tales.dev/">PLZ → Koordinaten</a>
            <a role="menuitem" href="https://paw-wiki.data-tales.dev/">Paw Patrole Wiki</a>
            <a role="menuitem" href="https://paw-quiz.data-tales.dev/">Paw Patrole Quiz</a>
            <a role="menuitem" href="https://hp-quiz.data-tales.dev/">Harry Potter Quiz</a>
            <a role="menuitem" href="https://worm-attack-3000.data-tales.dev/">Wurm Attacke 3000</a>
          </div>
      </div>

      <div class="header-actions">
        <div class="header-note" aria-label="Feedback Kontakt">
          <span class="header-note__label">Änderung / Kritik:</span>
          <a class="header-note__mail" href="mailto:info@data-tales.dev">info@data-tales.dev</a>
        </div>

        
        <button class="btn btn-ghost" id="themeToggle" type="button" aria-label="Theme umschalten">
          <span aria-hidden="true" id="themeIcon">☾</span>
          <span class="sr-only">Theme umschalten</span>
        </button>
      </div>
    </div>
  </header>

  <main id="main">
    <section class="section">
      <div class="container">
        <p class="kicker">Service</p>
        <h1>{{ meta.page_h1 }}</h1>
        <p class="lead">{{ meta.page_subtitle }}</p>

        {% if error %}
          <div class="card">
            <div style="font-weight:900; margin-bottom:8px;">Fehler</div>
            <p class="muted">{{ error }}</p>
          </div>
        {% else %}
          <p class="muted" style="margin-bottom:18px;">
            Angezeigt: <strong>{{ characters|length }}</strong> Charaktere (nur Einträge mit <code>profile_flat</code>).
          </p>

          <div class="grid" id="charGrid">
            {% for ch in characters %}
              <article class="card char-card" data-id="{{ ch.id }}">
                <button class="char-toggle" type="button"
                        aria-expanded="false"
                        aria-controls="details-{{ ch.id }}"
                        data-id="{{ ch.id }}">
                  <div class="char-title">{{ ch.name }}</div>

                  {% if ch.image_url %}
                    <div class="char-thumb">
                      <img src="{{ ch.image_url }}" alt="Bild von {{ ch.name }}" loading="lazy" />
                    </div>
                  {% else %}
                    <div class="char-thumb placeholder" aria-label="Kein Bild verfügbar">
                      Kein Bild
                    </div>
                  {% endif %}
                </button>

                <div class="char-details" id="details-{{ ch.id }}" hidden>
                  <dl class="kv">
                    {% for k, v in ch.profile_items %}
                      <dt>{{ k }}</dt>
                      <dd>{{ v }}</dd>
                    {% endfor %}
                  </dl>

                  <div class="attr">
                    {% if ch.source_page_url %}
                      <div><strong>Quelle:</strong> <a href="{{ ch.source_page_url }}" target="_blank" rel="noreferrer">{{ ch.source_page_url }}</a></div>
                    {% endif %}
                    {% if ch.source_attribution %}
                      <div style="margin-top:6px;">{{ ch.source_attribution }}</div>
                    {% else %}
                      <div style="margin-top:6px;">Hinweis: Bitte Lizenz/Attribution gemäß CC-BY-SA prüfen und beim Weitergeben nennen.</div>
                    {% endif %}
                  </div>
                </div>
              </article>
            {% endfor %}
          </div>
        {% endif %}
      </div>
    </section>
  </main>

  <footer class="site-footer">
    <div class="container footer-inner">
      <span class="muted">© <span id="year"></span> data-tales.dev</span>
      <span class="muted">Flask • Cloud Run</span>
    </div>
  </footer>

  <script>
    (function(){
    const dd = document.querySelector('[data-dropdown]');
    if(!dd) return;

    const btn = dd.querySelector('.nav-dropbtn');
    const menu = dd.querySelector('.nav-menu');

    function setOpen(isOpen){
      btn.setAttribute('aria-expanded', String(isOpen));
      if(isOpen){
        menu.hidden = false;
        dd.classList.add('open');
      }else{
        menu.hidden = true;
        dd.classList.remove('open');
      }
    }

    btn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      const isOpen = btn.getAttribute('aria-expanded') === 'true';
      setOpen(!isOpen);
    });

    document.addEventListener('click', (e) => {
      if(!dd.contains(e.target)) setOpen(false);
    });

    document.addEventListener('keydown', (e) => {
      if(e.key === 'Escape') setOpen(false);
    });

    // Wenn per Tab aus dem Dropdown rausnavigiert wird: schließen
    dd.addEventListener('focusout', () => {
      requestAnimationFrame(() => {
        if(!dd.contains(document.activeElement)) setOpen(false);
      });
    });

    // Initial geschlossen
    setOpen(false);
  })();
  (function(){
    // Theme toggle (Landing-Style): data-theme="light" setzen oder entfernen; localStorage key "theme"
    var root = document.documentElement;
    var btn = document.getElementById("themeToggle");
    var icon = document.getElementById("themeIcon");

    function applyTheme(theme){
      if(theme === "light"){
        root.setAttribute("data-theme", "light");
        if(icon) icon.textContent = "☀";
      } else {
        root.removeAttribute("data-theme");
        if(icon) icon.textContent = "☾";
      }
    }

    try {
      var saved = localStorage.getItem("theme");
      applyTheme(saved === "light" ? "light" : "dark");
    } catch(e) {
      applyTheme("dark");
    }

    if(btn){
      btn.addEventListener("click", function(){
        try {
          var isLight = root.getAttribute("data-theme") === "light";
          var next = isLight ? "dark" : "light";
          if(next === "light"){
            localStorage.setItem("theme", "light");
          } else {
            localStorage.removeItem("theme");
          }
          applyTheme(next);
        } catch(e) {}
      });
    }

    var y = document.getElementById("year");
    if(y) y.textContent = new Date().getFullYear();

    // Exclusive accordion for character cards
    var openId = null;

    function closeCard(card){
      if(!card) return;
      card.classList.remove("is-open");
      var btn = card.querySelector(".char-toggle");
      var details = card.querySelector(".char-details");
      if(btn) btn.setAttribute("aria-expanded", "false");
      if(details){
        details.style.maxHeight = "0px";
        details.hidden = true;
      }
    }

    function openCard(card){
      if(!card) return;
      card.classList.add("is-open");
      var btn = card.querySelector(".char-toggle");
      var details = card.querySelector(".char-details");
      if(btn) btn.setAttribute("aria-expanded", "true");
      if(details){
        details.hidden = false;
        // allow layout, then animate
        requestAnimationFrame(function(){
          var h = details.scrollHeight || 0;
          details.style.maxHeight = h + "px";
        });
      }
    }

    function toggleById(id){
      var card = document.querySelector('.char-card[data-id="'+ id +'"]');
      if(!card) return;

      if(openId && openId !== id){
        var prev = document.querySelector('.char-card[data-id="'+ openId +'"]');
        closeCard(prev);
        openId = null;
      }

      var isOpen = card.classList.contains("is-open");
      if(isOpen){
        closeCard(card);
        openId = null;
      } else {
        openCard(card);
        openId = id;
      }
    }

    document.addEventListener("click", function(ev){
      var t = ev.target;
      var btn = t && t.closest ? t.closest(".char-toggle") : null;
      if(!btn) return;
      var id = btn.getAttribute("data-id");
      if(!id) return;
      toggleById(id);
    });

    // keep open card height correct on resize
    window.addEventListener("resize", function(){
      if(!openId) return;
      var card = document.querySelector('.char-card[data-id="'+ openId +'"]');
      if(!card) return;
      var details = card.querySelector(".char-details");
      if(details && !details.hidden){
        details.style.maxHeight = (details.scrollHeight || 0) + "px";
      }
    });
  })();
  </script>
</body>
</html>
"""


def _build_nav() -> Tuple[List[Tuple[str, str]], bool]:
    cleaned = _clean_services(SERVICES)
    # limit to first 6
    show_more = len(cleaned) > 6
    return cleaned[:6], show_more


def _media_url_for_local_path(local_path: Optional[str]) -> Optional[str]:
    if not local_path or not isinstance(local_path, str):
        return None
    lp = local_path.strip().lstrip("/")

    # only serve files under images/
    if not lp.startswith(MEDIA_ALLOWED_PREFIX):
        return None
    return "/media/" + lp


def _render_page(error: Optional[str] = None) -> str:
    nav_links, show_more = _build_nav()
    chars_view = []

    if not error:
        ds = load_dataset()
        for ch in ds["characters"]:
            pf = ch.get("profile_flat") or {}
            # stable, readable key order
            items = sorted(((str(k), str(v)) for k, v in pf.items()), key=lambda kv: kv[0].lower())
            chars_view.append(
                {
                    "id": ch["id"],
                    "name": ch["name"],
                    "image_url": _media_url_for_local_path(ch.get("image_local_path")),
                    "profile_items": items,
                    "source_page_url": ch.get("source_page_url"),
                    "source_attribution": ch.get("source_attribution"),
                }
            )

    return render_template_string(
        TEMPLATE,
        meta=SERVICE_META,
        landing_url=LANDING_URL,
        cookbook_url=COOKBOOK_URL,
        nav_links=nav_links,
        show_more=show_more,
        error=error,
        characters=chars_view,
    )


@app.get("/")
def index() -> str:
    try:
        # Ensure dataset is loadable; errors handled cleanly
        _ = load_dataset()
        return _render_page(error=None)
    except Exception:
        # Avoid stack traces in UI
        msg = (
            "Datenset konnte nicht geladen werden. "
            "Prüfe, ob 'out_pawpatrol_characters/characters.json' im Service vorhanden ist "
            "oder setze DATA_JSON_PATH/DATA_BASE_DIR korrekt."
        )
        return _render_page(error=msg)


@app.get("/api/characters")
def api_characters():
    try:
        ds = load_dataset()
        out = []
        for ch in ds["characters"]:
            out.append(
                {
                    "id": ch["id"],
                    "name": ch["name"],
                    "image_url": _media_url_for_local_path(ch.get("image_local_path")),
                    "profile_flat": ch.get("profile_flat") or {},
                    "source_page_url": ch.get("source_page_url"),
                    "source_attribution": ch.get("source_attribution"),
                }
            )
        return jsonify({"ok": True, "count": len(out), "characters": out})
    except Exception:
        return jsonify({"ok": False, "error": "dataset not available"}), 500


@app.get("/api/characters/<cid>")
def api_character(cid: str):
    if not ID_RE.match(cid or ""):
        return jsonify({"ok": False, "error": "invalid id"}), 400
    try:
        ds = load_dataset()
        ch = ds["by_id"].get(cid)
        if not ch:
            return jsonify({"ok": False, "error": "not found"}), 404
        out = {
            "id": ch["id"],
            "name": ch["name"],
            "image_url": _media_url_for_local_path(ch.get("image_local_path")),
            "profile_flat": ch.get("profile_flat") or {},
            "source_page_url": ch.get("source_page_url"),
            "source_attribution": ch.get("source_attribution"),
        }
        return jsonify({"ok": True, "character": out})
    except Exception:
        return jsonify({"ok": False, "error": "dataset not available"}), 500


@app.get("/media/<path:relpath>")
def media(relpath: str):
    # Only allow images/* under DATA_BASE_DIR
    relpath = (relpath or "").lstrip("/")
    if not relpath.startswith(MEDIA_ALLOWED_PREFIX):
        return jsonify({"ok": False, "error": "not found"}), 404

    safe_abs = _safe_realpath(DATA_BASE_DIR, relpath)
    if not safe_abs or not os.path.exists(safe_abs) or not os.path.isfile(safe_abs):
        return jsonify({"ok": False, "error": "not found"}), 404

    resp: Response = send_file(safe_abs)
    resp.headers["Cache-Control"] = f"public, max-age={MEDIA_MAX_AGE_SECONDS}, immutable"
    return resp


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)
