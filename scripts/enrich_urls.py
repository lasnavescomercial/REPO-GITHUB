#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, unicodedata, time, sys, csv, math
from pathlib import Path
import requests, pandas as pd
from bs4 import BeautifulSoup

# Official domains we trust
OFFICIAL = {
    "JIMTEN": ["jimten.com", "catalogo.jimten.com"],
    "ESPA":   ["espa.com", "psp.espa.com", "espapumps.co.uk"],
    "GENEBRE":["genebre.com"],
}

# Common aliases as they often appear in real spreadsheets
ALIASES = {
    "JIMTEN":  ["JIMTEN", "JIMTEN SA", "JIMTEN, S.A.", "JIMTEN S.A", "JIMTEN S A"],
    "ESPA":    ["ESPA", "ESPA 2020", "ESPA PUMPS", "ESPA PUMPS IBERICA", "ESPA PUMPS IBÉRICA"],
    "GENEBRE": ["GENEBRE", "GENEBRE SA", "GENEBRE, S.A.", "GENEBRE S.A", "GENEBRE S A"],
    # Intermediaries you may have (skip brand here, but try all brands as fallback)
    "":        ["FAMARA", "LAS NAVES", "ALMACENES", "DISTRIBUIDOR"]
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
    # heuristic contains
    for canon in ("JIMTEN","ESPA","GENEBRE"):
        if canon in n:
            return canon
    return ""

def ddg_search(domain:str, query:str, session:requests.Session, max_hits=6):
    url = "https://duckduckgo.com/html/"
    q = f"site:{domain} {query}"
    r = session.get(url, params={"q": q}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    hits = []
    for a in soup.select("a.result__a"):
        href = a.get("href")
        if href and domain in href:
            hits.append(href)
            if len(hits) >= max_hits:
                break
    return hits

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
        # 1) og:image
        og = soup.select_one('meta[property="og:image"], meta[name="og:image"]')
        if og and og.get("content"):
            candidate = requests.compat.urljoin(url, og["content"])
            ct = session.get(candidate, timeout=30, stream=True).headers.get("Content-Type","").lower()
            if ct.startswith("image/"):
                return candidate
        # 2) fallback: biggest inline image by width*height (if attrs available)
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

def try_enrich_for_brand(prov_canon:str, ref:str, art:str, session:requests.Session):
    # Normalize reference variants (remove dots/spaces, also try dashless)
    ref_variants = {ref}
    ref_variants.add(re.sub(r"[.\s]+","", ref))
    ref_variants.add(ref.replace("-", ""))
    for d in OFFICIAL.get(prov_canon, []):
        for base in ref_variants:
            for q in (base, f"{base} {art}"):
                hits = ddg_search(d, q, session)
                if hits:
                    # pick first likely product page
                    for h in hits:
                        bad = ("/search", "/busc", "/tag/", "/category", "/noticias", "/blog", "/catalogo_corporativo")
                        if any(s in h.lower() for s in bad):
                            continue
                        pdf = pick_pdf_from_page(h, session)
                        img = pick_image_from_page(h, session)
                        if pdf or img:
                            return img, pdf, h, d, q
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

    # ensure URL columns exist
    for key in ("img","pdf"):
        if COLS[key] not in df.columns:
            df[COLS[key]] = ""

    s = requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0"})

    total = len(df); filled = 0
    report_rows = []

    for i, row in df.iterrows():
        cod_art  = row.get(COLS["cod_art"])
        ref      = str(row.get(COLS["refprov"]) or "").strip()
        art      = str(row.get(COLS["art"]) or "").strip()
        prov_raw = str(row.get(COLS["prov"]) or "").strip()
        prov     = canonical_brand(prov_raw)

        need_img = is_empty(row.get(COLS["img"]))
        need_pdf = is_empty(row.get(COLS["pdf"]))

        status = "skipped"
        found_img = None; found_pdf = None; page = None; used_brand = prov; dom = None; q = None

        if not (need_img or need_pdf):
            status = "already had URLs"
        else:
            # If brand is recognized, try that brand first
            tried = False
            if prov in OFFICIAL:
                img, pdf, page, dom, q = try_enrich_for_brand(prov, ref, art, s)
                tried = True
                if img or pdf:
                    if need_img and img: df.at[i, COLS["img"]] = img
                    if need_pdf and pdf: df.at[i, COLS["pdf"]] = pdf
                    status = "filled"
                    found_img, found_pdf, used_brand = img, pdf, prov
                    filled += 1
            # Fallback: try all brands if nothing yet (handles intermediaries like FAMARA)
            if status != "filled":
                for prov2 in ("JIMTEN","ESPA","GENEBRE"):
                    img, pdf, page, dom, q = try_enrich_for_brand(prov2, ref, art, s)
                    if img or pdf:
                        if need_img and img: df.at[i, COLS["img"]] = img
                        if need_pdf and pdf: df.at[i, COLS["pdf"]] = pdf
                        status = "filled"
                        found_img, found_pdf, used_brand = img, pdf, prov2
                        filled += 1
                        break
                if status != "filled" and tried:
                    status = "no match on brand"
                elif status != "filled":
                    status = "no match any brand"

        report_rows.append({
            "row": i+1,
            "cod_articulo_naves": cod_art,
            "ref_proveedor": ref,
            "proveedor_raw": prov_raw,
            "brand_used": used_brand,
            "query_domain": dom or "",
            "query": q or "",
            "product_page": page or "",
            "found_image": found_img or "",
            "found_pdf": found_pdf or "",
            "status": status
        })

        time.sleep(0.6)  # be polite

    df.to_excel(args.out, index=False)

    # Write CSV report
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(report_rows[0].keys()))
        w.writeheader()
        w.writerows(report_rows)

    print(f"[OK] Enrichment done. Rows: {total}. Rows updated: {filled}.")
    print(f"[OK] Outputs: {args.out} and {args.report}")

if __name__ == "__main__":
    main()
