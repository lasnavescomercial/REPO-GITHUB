#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, unicodedata, time, sys, csv, math, urllib.parse
from pathlib import Path
import requests, pandas as pd
from bs4 import BeautifulSoup

print("[INFO] Engine: GOOGLE CSE (webwide)")

# Aliases de marcas
ALIASES = {
    "JIMTEN":  ["JIMTEN", "JIMTEN SA", "JIMTEN, S.A.", "JIMTEN S.A", "JIMTEN S A"],
    "ESPA":    ["ESPA", "ESPA 2020", "ESPA PUMPS", "ESPA PUMPS IBERICA", "ESPA PUMPS IBÉRICA"],
    "GENEBRE": ["GENEBRE", "GENEBRE SA", "GENEBRE, S.A.", "GENEBRE S.A", "GENEBRE S A"],
    "":        ["FAMARA", "LAS NAVES", "ALMACENES", "DISTRIBUIDOR", "PROVEEDOR"]
}

COLS = {
    "cod_art": "Cód. Articulo Naves",
    "refprov": "Referencia Proveedor",
    "art":     "Artículo",
    "prov":    "Proveedor",
    "codprov": "Cód. Proveedor",
    "img":     "URL Imagen Oficial",
    "pdf":     "URL Ficha Técnica Oficial",
}

BLACKLIST = {
    "amazon.", "ebay.", "aliexpress.", "alibaba.", "leroymerlin.", "manomano.",
    "pinterest.", "facebook.", "instagram.", "youtube.", "issuu.", "scribd.",
    "mercadolibre.", "wikipedia.", "reddit.", "x.com", "tiktok.", "linkedin."
}

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
    n = norm_text(raw)
    if not n:
        return ""
    for canon, variants in ALIASES.items():
        for v in variants:
            if norm_text(v) == n:
                return canon
    for canon in ("JIMTEN","ESPA","GENEBRE","FLUIDRA"):
        if canon in n:
            return canon
    return ""

# -------- Google Programmable Search (CSE) - web completa --------
def google_search_all(query: str, session: requests.Session, max_hits=8):
    key = os.environ.get("GOOGLE_CSE_KEY")
    cx  = os.environ.get("GOOGLE_CSE_CX")
    if not key or not cx:
        raise RuntimeError("Missing GOOGLE_CSE_KEY or GOOGLE_CSE_CX (set repo secrets).")
    endpoint = "https://www.googleapis.com/customsearch/v1"
    params = {"key": key, "cx": cx, "q": query, "num": max_hits}
    r = session.get(endpoint, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    hits = []
    for item in data.get("items", []) or []:
        url = item.get("link")
        if url:
            hits.append(url)
    return hits
# -----------------------------------------------------------------

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
        og = soup.select_one('meta[property="og:image"], meta[name="og:image"]')
        if og and og.get("content"):
            candidate = requests.compat.urljoin(url, og["content"])
            ct = session.get(candidate, timeout=30, stream=True).headers.get("Content-Type","").lower()
            if ct.startswith("image/"):
                return candidate
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

def domain_host(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""

def is_blacklisted(host: str) -> bool:
    return any(bad in host for bad in BLACKLIST)

def looks_like_brand_site(host: str, brand: str) -> bool:
    if not brand: return False
    return brand.lower() in host

def try_enrich_webwide(brand: str, ref: str, art: str, session: requests.Session):
    ref_vars = {ref, re.sub(r"[.\s]+","", ref), ref.replace("-", "")}
    queries = []
    for rv in ref_vars:
        queries += [rv, f"{rv} {art}".strip()]
        if brand:
            queries += [f"{brand} {rv}", f"{brand} {rv} {art}".strip()]

    candidates, seen = [], set()
    for q in queries:
        if not q.strip(): 
            continue
        hits = google_search_all(q, session)
        for u in hits:
            if u not in seen:
                seen.add(u); candidates.append(u)

    for prefer_brand in (True, False):
        for url in candidates:
            host = domain_host(url)
            if is_blacklisted(host): 
                continue
            if prefer_brand and brand and not looks_like_brand_site(host, brand):
                continue
            pdf = pick_pdf_from_page(url, session)
            img = pick_image_from_page(url, session)
            if pdf or img:
                return img, pdf, url, host, "brand-pass" if prefer_brand else "open-pass"
    return None, None, None, None, None

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="data/RESUMEN_CATALOGO.xlsx")
    ap.add_argument("--out",    default="data/RESUMEN_CATALOGO_READY.xlsx")
    ap.add_argument("--report", default="data/ENRICHMENT_REPORT.csv")
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

    total = len(df); filled = 0
    rows = []

    for i, row in df.iterrows():
        cod_art  = row.get(COLS["cod_art"])
        ref      = str(row.get(COLS["refprov"]) or "").strip()
        art      = str(row.get(COLS["art"]) or "").strip()
        prov_raw = str(row.get(COLS["prov"]) or "").strip()
        brand    = canonical_brand(prov_raw)

        need_img = is_empty(row.get(COLS["img"]))
        need_pdf = is_empty(row.get(COLS["pdf"]))

        status = "skipped"
        found_img = None; found_pdf = None; page = None; used_pass = None; host = None

        if not (need_img or need_pdf):
            status = "already had URLs"
        else:
            img, pdf, page, host, used_pass = try_enrich_webwide(brand, ref, art, s)
            if img or pdf:
                if need_img and img: df.at[i, COLS["img"]] = img
                if need_pdf and pdf: df.at[i, COLS["pdf"]] = pdf
                status = "filled"; found_img, found_pdf = img, pdf; filled += 1
            else:
                status = "no match"

        rows.append({
            "row": i+1,
            "cod_articulo_naves": cod_art,
            "ref_proveedor": ref,
            "proveedor_raw": prov_raw,
            "brand_detected": brand or "",
            "chosen_host": host or "",
            "search_pass": used_pass or "",
            "product_page": page or "",
            "found_image": found_img or "",
            "found_pdf": found_pdf or "",
            "status": status
        })
        time.sleep(0.3)

    df.to_excel(args.out, index=False)

    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)

    print(f"[OK] Enrichment done. Rows: {total}. Rows updated: {filled}.")
    print(f"[OK] Outputs: {args.out} and {args.report}")

if __name__ == "__main__":
    main()
