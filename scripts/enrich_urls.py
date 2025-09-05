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
