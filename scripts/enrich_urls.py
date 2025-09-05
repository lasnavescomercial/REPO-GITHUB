#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, unicodedata, time, sys
from pathlib import Path
import requests, pandas as pd
from bs4 import BeautifulSoup

OFFICIAL = {
    "JIMTEN": ["jimten.com", "catalogo.jimten.com"],
    "ESPA":   ["espa.com", "espapumps.co.uk"],
    "GENEBRE":["genebre.com"],
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

def norm_brand(s:str)->str:
    s = (s or "").strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Z0-9]+"," ",s).strip()
    if "JIMTEN" in s: return "JIMTEN"
    if "ESPA" in s: return "ESPA"
    if "GENEBRE" in s: return "GENEBRE"
    return s

def ddg_search(domain:str, query:str, session:requests.Session, max_hits=5):
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
                    w = int(img.get("width") or 0)
                    h = int(img.get("height") or 0)
                    area = w*h
                    if area > best_area:
                        best_area, best = area, candidate
            except Exception:
                continue
        return best
    except Exception:
        return None

def enrich_row(row, session):
    prov = norm_brand(row.get(COLS["prov"]))
    ref  = str(row.get(COLS["refprov"]) or "").strip()
    art  = str(row.get(COLS["art"]) or "").strip()
    if not prov or prov not in OFFICIAL or not ref:
        return None, None
    domains = OFFICIAL[prov]
    found_page = None
    pdf_url = None
    img_url = None
    for d in domains:
        for q in (ref, f"{ref} {art}"):
            hits = ddg_search(d, q, session)
            for h in hits:
                if any(s in h.lower() for s in ("/search", "/busc", "/tag/", "/category", "/noticias", "/blog")):
                    continue
                found_page = h
                break
            if found_page:
                break
        if found_page:
            pdf_url = pick_pdf_from_page(found_page, session)
            img_url = pick_image_from_page(found_page, session)
            if pdf_url or img_url:
                break
        time.sleep(1)
    return img_url, pdf_url

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="data/RESUMEN_CATALOGO.xlsx")
    ap.add_argument("--out",    default="data/RESUMEN_CATALOGO_READY.xlsx")
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
    for i, row in df.iterrows():
        need_img = not str(row.get(COLS["img"]) or "").strip()
        need_pdf = not str(row.get(COLS["pdf"]) or "").strip()
        if need_img or need_pdf:
            img, pdf = enrich_row(row, s)
            changed = False
            if img and need_img:
                df.at[i, COLS["img"]] = img; changed = True
            if pdf and need_pdf:
                df.at[i, COLS["pdf"]] = pdf; changed = True
            if changed: filled += 1
            time.sleep(0.8)

    df.to_excel(args.out, index=False)
    print(f"[OK] Enrichment done. Rows: {total}. Rows updated: {filled}. Output: {args.out}")

if __name__ == "__main__":
    main()
