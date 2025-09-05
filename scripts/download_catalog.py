#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, sys, unicodedata, zipfile, math
from pathlib import Path
import requests, pandas as pd

def is_empty(val) -> bool:
    if val is None: return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)): return True
    s = str(val).strip()
    return s == "" or s.lower() == "nan" or s == "None"

def sanitize(s):
    if s is None: return ""
    s = str(s).strip()
    s = re.sub(r'[\\/:*?"<>|]+', "-", s)
    return s

def ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def ext_from_ct(ct):
    if not ct: return None
    ct = ct.lower()
    if "pdf" in ct: return ".pdf"
    if "jpeg" in ct or "jpg" in ct: return ".jpg"
    if "png" in ct: return ".png"
    if "gif" in ct: return ".gif"
    if "webp" in ct: return ".webp"
    return None

def download(url, dest: Path, session, force_jpg=False):
    try:
        with session.get(url, timeout=45, stream=True) as r:
            if r.status_code != 200:
                return False, f"HTTP {r.status_code}"
            ct = r.headers.get("Content-Type", "")
            ext = ext_from_ct(ct)
            if ext: dest = dest.with_suffix(ext)
            ensure_parent(dest)
            with open(dest, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk: f.write(chunk)
        if force_jpg and "image" in (ct or "").lower() and dest.suffix.lower() not in (".jpg",".jpeg"):
            try:
                from PIL import Image
                img = Image.open(dest).convert("RGB")
                jpg = dest.with_suffix(".jpg")
                img.save(jpg, "JPEG", quality=92, optimize=True)
                dest.unlink(missing_ok=True)
            except Exception:
                pass
        return True, "OK"
    except Exception as e:
        return False, str(e)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="data/RESUMEN_CATALOGO.xlsx", help="Excel path")
    args = ap.parse_args()

    EXCEL_PATH = args.excel
    OUTPUT_ROOT = Path("CATALOGO")
    ZIP_NAME = "CATALOGO.zip"

    COL_COD_ART = "Cód. Articulo Naves"
    COL_REF_PROV = "Referencia Proveedor"
    COL_PROVEEDOR = "Proveedor"
    COL_COD_PROV = "Cód. Proveedor"
    COL_URL_IMG = "URL Imagen Oficial"
    COL_URL_FICHA = "URL Ficha Técnica Oficial"

    if not os.path.exists(EXCEL_PATH):
        print(f"[ERROR] Excel not found: {EXCEL_PATH}")
        sys.exit(1)

    df = pd.read_excel(EXCEL_PATH, sheet_name=0)
    for c in [COL_COD_ART, COL_REF_PROV, COL_PROVEEDOR, COL_COD_PROV, COL_URL_IMG, COL_URL_FICHA]:
        if c not in df.columns:
            print(f"[ERROR] Missing column: {c}")
            sys.exit(1)

    s = requests.Session()
    s.headers.update({"User-Agent":"Mozilla/5.0"})

    total = len(df)
    tried = 0

    for i, row in df.iterrows():
        cod_art = sanitize(row.get(COL_COD_ART, ""))
        ref_prov = sanitize(row.get(COL_REF_PROV, ""))
        proveedor = sanitize(row.get(COL_PROVEEDOR, ""))
        cod_prov = sanitize(row.get(COL_COD_PROV, ""))

        # Treat NaN / blanks as empty
        uimg = "" if is_empty(row.get(COL_URL_IMG)) else str(row.get(COL_URL_IMG)).strip()
        updf = "" if is_empty(row.get(COL_URL_FICHA)) else str(row.get(COL_URL_FICHA)).strip()

        if not cod_art or not ref_prov or not proveedor or not cod_prov:
            print(f'[{i+1}/{total}] SKIP: missing key fields')
            continue

        base = f"{cod_art} - {ref_prov}"
        img_dir = OUTPUT_ROOT / "IMAGENES" / f"{cod_prov} - {proveedor}"
        pdf_dir = OUTPUT_ROOT / "FICHAS"  / f"{cod_prov} - {proveedor}"

        if uimg:
            ok, msg = download(uimg, img_dir / (base + ".jpg"), s, force_jpg=True)
            print(f"[IMG] {base}: {msg}")
            tried += 1
        if updf:
            ok, msg = download(updf, pdf_dir / (base + ".pdf"), s, force_jpg=False)
            print(f"[PDF] {base}: {msg}")
            tried += 1

    with zipfile.ZipFile(ZIP_NAME, "w", zipfile.ZIP_DEFLATED) as z:
        for folder, _, files in os.walk(OUTPUT_ROOT):
            for f in files:
                full = os.path.join(folder, f)
                arc = os.path.relpath(full, start=os.path.dirname(OUTPUT_ROOT))
                z.write(full, arc)

    print(f"[OK] Done. Rows: {total}. Downloads attempted (with URLs): {tried}.")

if __name__ == "__main__":
    try:
        from PIL import Image  # optional
    except Exception:
        pass
    main()
