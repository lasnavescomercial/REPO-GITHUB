#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enriquecedor de URLs (imagen y ficha técnica) usando Google Custom Search (webwide).

Mejoras:
- Hints por marca: si el proveedor es de un grupo conocido (p.ej. FLUIDRA),
  se priorizan búsquedas "site:<dominio>" en webs oficiales antes de ir a toda la web.
- Más variantes de consulta (ref sin guiones/puntos/espacios, combos con artículo y marca).
- Exclusión automática de FAMARA.
- Filtro literal --provider-contains (coincidencia por subcadena normalizada en "Proveedor").

Requiere secrets en el workflow:
  GOOGLE_CSE_KEY, GOOGLE_CSE_CX
"""

import os, re, unicodedata, time, sys, csv, math, urllib.parse
from pathlib import Path
import requests, pandas as pd
from bs4 import BeautifulSoup

print("[INFO] Engine: GOOGLE CSE (webwide) | Filtro: --provider-contains | Regla: excluir FAMARA | Hints por marca activos")

# -------------------- Config --------------------

BLACKLIST = {
    "amazon.", "ebay.", "aliexpress.", "alibaba.", "leroymerlin.", "manomano.",
    "pinterest.", "facebook.", "instagram.", "youtube.", "issuu.", "scribd.",
    "mercadolibre.", "wikipedia.", "reddit.", "x.com", "tiktok.", "linkedin."
}

# Aliases para detectar marca a partir del campo "Proveedor"
ALIASES = {
    "JIMTEN":  ["JIMTEN", "JIMTEN SA", "JIMTEN, S.A.", "JIMTEN S.A", "JIMTEN S A"],
    "ESPA":    ["ESPA", "ESPA 2020", "ESPA PUMPS", "ESPA PUMPS IBERICA", "ESPA PUMPS IBÉRICA"],
    "GENEBRE": ["GENEBRE", "GENEBRE SA", "GENEBRE, S.A.", "GENEBRE S.A", "GENEBRE S A"],
    # Grupo FLUIDRA (marcas frecuentes del grupo)
    "FLUIDRA": ["FLUIDRA", "FLUIDRA SA", "FLUIDRA S.A", "ZODIAC", "ZODIAC FLUIDRA",
                "ASTRALPOOL", "CTX", "CTX PROFESSIONAL", "CEPEX"],
    "":        ["LAS NAVES", "ALMACENES", "DISTRIBUIDOR", "PROVEEDOR"]
}

# Pistas por marca → dominios a priorizar
BRAND_HINTS = {
    "FLUIDRA": [
        "fluidra.com", "astralpool.com", "cepex.com",
        "ctxprofessional.com", "zodiacpoolcare.com", "zodiac.com"
    ],
    "JIMTEN":  ["jimten.com"],           # ajusta si tienes dominio oficial más preciso
    "ESPA":    ["espa.com", "espa.es"],  # idem
    "GENEBRE": ["genebre.es", "genebre.com"],
}

# Proveedores a excluir (normalizado)
EXCLUDE_PROVIDERS = {"FAMARA"}

# Nombres de columnas
COLS = {
    "cod_art": "Cód. Articulo Naves",
    "refprov": "Referencia Proveedor",
    "art":     "Artículo",
    "prov":    "Proveedor",
    "codprov": "Cód. Proveedor",
    "img":     "URL Imagen Oficial",
    "pdf":     "URL Ficha Técnica Oficial",
}

# -------------------- Utilidades --------------------

def is_empty(val) -> bool:
    if val is None: return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)): return True
    s = str(val).strip()
    return s == "" or s.lower() == "nan" or s == "None"

def norm_text(s:str)->str:
    s = str(s or "").strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^A-Z0-9]+", " ", s).strip()

def canonical_brand(raw:str)->str:
    """Intenta mapear el texto de proveedor a marca canónica."""
    n = norm_text(raw)
    if not n:
        return ""
    for canon, variants in ALIASES.items():
        for v in variants:
            if norm_text(v) == n:
                return canon
    # heurística de "contiene"
    for canon in ("JIMTEN","ESPA","GENEBRE","FLUIDRA"):
        if canon in n:
            return canon
    return ""

def is_excluded_provider(prov_raw: str) -> bool:
    n = norm_text(prov_raw)
    return any(word in n for word in (norm_text(x) for x in EXCLUDE_PROVIDERS))

def host_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""

def is_blacklisted(host: str) -> bool:
    return any(bad in host for bad in BLACKLIST)

def looks_like_brand_site(host: str, brand: str) -> bool:
    if not brand:
        return False
    brand = brand.lower()
    if brand in host:
        return True
    # si hay hints definidos, considera match si host termina en alguno de ellos
    for dom in BRAND_HINTS.get(brand.upper(), []):
        d = dom.lower()
        if host.endswith(d) or host == d or host.endswith("." + d):
            return True
    return False

# -------------------- Google CSE --------------------

class QuotaExceeded(Exception): pass

def google_search(session, key, cx, q, num=8, sleep_s=1.1):
    endpoint = "https://www.googleapis.com/customsearch/v1"
    r = session.get(endpoint, params={"key":key, "cx":cx, "q":q, "num":num}, timeout=30)
    if r.status_code == 429:
        raise QuotaExceeded("429 Too Many Requests from Google CSE")
    r.raise_for_status()
    data = r.json()
    time.sleep(sleep_s)
    hits = []
    for item in data.get("items", []) or []:
        url = item.get("link")
        if url:
            hits.append(url)
    return hits

# -------------------- Scraping recursos --------------------

def pick_pdf_from_page(url:str, session:requests.Session):
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        if (r.headers.get("Content-Type","").lower().startswith("application/pdf")):
            return url
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href]"):
            href = a["href"]
            if ".pdf" in href.lower():
                pdf = requests.compat.urljoin(url, href)
                head = session.get(pdf, timeout=30, stream=True)
                ct = head.headers.get("Content-Type","").lower()
                if "application/pdf" in ct:
                    return pdf
    except Exception:
        return None
    return None

def pick_image_from_page(url:str, session:requests.Session):
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        if r.headers.get("Content-Type","").lower().startswith("image/"):
            return url
        soup = BeautifulSoup(r.text, "lxml")
        # og:image primero
        og = soup.select_one('meta[property="og:image"], meta[name="og:image"]')
        if og and og.get("content"):
            candidate = requests.compat.urljoin(url, og["content"])
            ct = session.get(candidate, timeout=30, stream=True).headers.get("Content-Type","").lower()
            if ct.startswith("image/"):
                return candidate
        # por tamaño (si vienen width/height)
        best = None; best_area = 0
        for img in soup.select("img[src]"):
            src = img["src"]
            candidate = requests.compat.urljoin(url, src)
            if ".svg" in candidate.lower():
                continue
            try:
                head = session.get(candidate, timeout=30, stream=True)
                ct = head.headers.get("Content-Type","").lower()
                if ct.startswith("image/"):
                    w = int(img.get("width") or 0); h = int(img.get("height") or 0)
                    area = w*h
                    if area > best_area:
                        best_area, best = area, candidate
            except Exception:
                continue
        return best
    except Exception:
        return None

# -------------------- Construcción de queries --------------------

def ref_variants(ref: str):
    """Genera variantes robustas de la referencia."""
    ref = (ref or "").strip()
    if not ref:
        return set()
    v = set()
    v.add(ref)
    v.add(ref.replace("-", ""))                  # sin guiones
    v.add(re.sub(r"[.\s]+", "", ref))           # sin puntos/espacios
    v.add(ref.replace(" ", ""))                 # sin espacios
    v.add(ref.replace(".", ""))                 # sin puntos
    return {x for x in v if x}

def build_queries(brand: str, ref: str, art: str):
    """Crea lista ordenada de consultas (sin site:)."""
    queries = []
    rset = ref_variants(ref) or {""}
    art = (art or "").strip()
    brand = (brand or "").strip()
    for rv in rset:
        if rv:
            queries.append(rv)
            if art:    queries.append(f"{rv} {art}")
            if brand:  queries.append(f"{brand} {rv}")
            if brand and art: queries.append(f"{brand} {rv} {art}")
    # si no hay ref, usa artículo con marca
    if not ref and art:
        queries.append(art)
        if brand: queries.append(f"{brand} {art}")
    # dedup manteniendo orden
    seen, ordered = set(), []
    for q in queries:
        if q not in seen:
            seen.add(q); ordered.append(q)
    return ordered

def build_site_queries(domains, brand: str, ref: str, art: str):
    """Crea queries con 'site:<dom>' primero (prioriza oficiales)."""
    base = build_queries(brand, ref, art)
    site_qs = []
    for dom in domains or []:
        for q in base:
            site_qs.append(f"site:{dom} {q}")
    return site_qs

# -------------------- Enriquecimiento por fila --------------------

def try_enrich_with_hints(brand: str, ref: str, art: str, session, key, cx, sleep_s):
    """Primero busca en dominios oficiales (hints), luego en la web general."""
    candidates, seen = [], set()

    # 1) Site-hints si hay lista para esa marca
    domains = BRAND_HINTS.get((brand or "").upper(), [])
    if domains:
        for q in build_site_queries(domains, brand, ref, art):
            for u in google_search(session, key, cx, q, num=8, sleep_s=sleep_s):
                if u not in seen:
                    seen.add(u); candidates.append(("hint", u))

    # 2) Webwide
    for q in build_queries(brand, ref, art):
        for u in google_search(session, key, cx, q, num=8, sleep_s=sleep_s):
            if u not in seen:
                seen.add(u); candidates.append(("web", u))

    # Escoger recursos
    for pass_name, url in candidates:
        h = host_of(url)
        if is_blacklisted(h):
            continue
        # en la pasada "hint" somos más permisivos con host (ya va filtrado por site:)
        if pass_name == "web" and brand and not looks_like_brand_site(h, brand):
            # para webwide, preferimos hosts que parezcan de marca
            continue
        pdf = pick_pdf_from_page(url, session)
        img = pick_image_from_page(url, session)
        if pdf or img:
            return img, pdf, url, h, pass_name
    return None, None, None, None, None

# -------------------- Programa principal --------------------

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="data/RESUMEN_CATALOGO.xlsx")
    ap.add_argument("--out",    default="data/RESUMEN_CATALOGO_READY.xlsx")
    ap.add_argument("--report", default="data/ENRICHMENT_REPORT.csv")
    ap.add_argument("--limit",  type=int, default=0, help="Máx filas (0=todas). Suele usarse 0 con workflow de subset.")
    ap.add_argument("--offset", type=int, default=0, help="Inicio (0-based)")
    ap.add_argument("--sleep-ms", type=int, default=1100, help="Pausa entre consultas CSE (ms)")
    ap.add_argument("--provider-contains", default="", help='Procesar SOLO filas cuyo "Proveedor" contenga este texto (normalizado). Vacío = todas')
    args = ap.parse_args()

    if not os.path.exists(args.excel):
        print(f"[ERROR] Excel not found: {args.excel}")
        sys.exit(1)

    df = pd.read_excel(args.excel, sheet_name=0)
    for key in ("img","pdf"):
        if COLS[key] not in df.columns:
            df[COLS[key]] = ""

    s = requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0"})
    sleep_s = max(0.0, args.sleep_ms / 1000.0)

    # Secrets
    key = os.environ.get("GOOGLE_CSE_KEY")
    cx  = os.environ.get("GOOGLE_CSE_CX")
    if not key or not cx:
        print("[ERROR] Falta GOOGLE_CSE_KEY o GOOGLE_CSE_CX en secrets.")
        sys.exit(1)

    prov_filter = norm_text(args.provider_contains)
    total = len(df); filled = 0
    rows = []
    start = max(0, int(args.offset))
    end = total if int(args.limit) == 0 else min(total, start + int(args.limit))

    print(f"[INFO] Processing rows {start}..{end-1} of {total}  (provider_contains='{args.provider_contains}')")

    class StopRun(Exception): pass

    try:
        for i in range(start, end):
            row = df.iloc[i]
            cod_art  = row.get(COLS["cod_art"])
            ref      = str(row.get(COLS["refprov"]) or "").strip()
            art      = str(row.get(COLS["art"]) or "").strip()
            prov_raw = str(row.get(COLS["prov"]) or "").strip()
            brand    = canonical_brand(prov_raw)  # puede salir "" si el proveedor no encaja con ALIASES

            # Excluir FAMARA
            if is_excluded_provider(prov_raw):
                rows.append({
                    "row": i+1, "cod_articulo_naves": cod_art, "ref_proveedor": ref,
                    "proveedor_raw": prov_raw, "brand_detected": brand or "",
                    "chosen_host": "", "search_pass": "proveedor_excluido",
                    "product_page": "", "found_image": "", "found_pdf": "", "status": "skipped_by_rule"
                })
                continue

            # Filtro literal por proveedor (normalizado)
            if prov_filter and prov_filter not in norm_text(prov_raw):
                rows.append({
                    "row": i+1, "cod_articulo_naves": cod_art, "ref_proveedor": ref,
                    "proveedor_raw": prov_raw, "brand_detected": brand or "",
                    "chosen_host": "", "search_pass": "skipped_provider_filter",
                    "product_page": "", "found_image": "", "found_pdf": "", "status": "skipped_by_provider"
                })
                continue

            need_img = is_empty(row.get(COLS["img"]))
            need_pdf = is_empty(row.get(COLS["pdf"]))
            status = "skipped"; found_img = found_pdf = page = used_pass = host = None

            if not (need_img or need_pdf):
                status = "already had URLs"
            else:
                # Si el filtro de proveedor dice FLUIDRA pero brand= "", fuerzo brand "FLUIDRA"
                # para que activen sus hints (útil cuando Proveedor es "Fluidra Commercial S.A.U." etc.)
                if not brand and prov_filter and "FLUIDRA" in prov_filter:
                    brand = "FLUIDRA"

                try:
                    img, pdf, page, host, used_pass = try_enrich_with_hints(brand, ref, art, s, key, cx, sleep_s)
                except QuotaExceeded as e:
                    print(f"[WARN] {e}. Guardando progreso parcial…")
                    raise StopRun()
                if img or pdf:
                    if need_img and img: df.at[i, COLS["img"]] = img
                    if need_pdf and pdf: df.at[i, COLS["pdf"]] = pdf
                    filled += 1
                    status = "filled"; found_img, found_pdf = img, pdf
                else:
                    status = "no match"

            rows.append({
                "row": i+1, "cod_articulo_naves": cod_art, "ref_proveedor": ref,
                "proveedor_raw": prov_raw, "brand_detected": brand or "",
                "chosen_host": host or "", "search_pass": used_pass or "",
                "product_page": page or "", "found_image": found_img or "",
                "found_pdf": found_pdf or "", "status": status
            })

    except StopRun:
        pass
    finally:
        df.to_excel(args.out, index=False)
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        if rows:
            with open(args.report, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader(); w.writerows(rows)
        print(f"[OK] Rows with URLs (filled): {filled}")
        print(f"[OK] Outputs: {args.out} and {args.report}")

if __name__ == "__main__":
    main()
