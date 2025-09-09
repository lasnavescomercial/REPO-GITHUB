#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga imágenes (→ .jpg) y fichas técnicas (.pdf) desde el Excel enriquecido.

- Estructura de salida:
  CATALOGO/
    IMAGENES/<Cód. Proveedor - Proveedor>/<Cód. Articulo Naves - Referencia Proveedor>.jpg
    FICHAS  /<Cód. Proveedor - Proveedor>/<Cód. Articulo Naves - Referencia Proveedor>.pdf

- Filtros:
  --provider-contains "FLUIDRA"  → procesa SOLO filas cuyo campo "Proveedor" contenga ese texto (normalizado).
  Exclusión automática de "FAMARA" como proveedor (no descarga nada de esas filas).

Requisitos:
  pip install requests pandas openpyxl pillow
"""

import argparse
import io
import math
import mimetypes
import os
import re
import unicodedata
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

import pandas as pd
import requests
from PIL import Image


# -------------------- Config --------------------

COLS = {
    "cod_art": "Cód. Articulo Naves",
    "refprov": "Referencia Proveedor",
    "art":     "Artículo",
    "prov":    "Proveedor",
    "codprov": "Cód. Proveedor",
    "img":     "URL Imagen Oficial",
    "pdf":     "URL Ficha Técnica Oficial",
}

EXCLUDE_PROVIDERS = {"FAMARA"}  # normalizado

DEFAULT_OUT_DIR = "CATALOGO"
DEFAULT_ZIP     = "CATALOGO.zip"


# -------------------- Utilidades --------------------

def norm_text(s: str) -> str:
    s = str(s or "").strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^A-Z0-9]+", " ", s).strip()


def is_empty(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return True
    s = str(val).strip()
    return s == "" or s.lower() == "nan" or s == "None"


def safe_name(s: str) -> str:
    # Mantiene letras, números, guiones y espacios; colapsa repeticiones
    s = str(s or "").strip()
    s = re.sub(r"[\\/:*?\"<>|]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def is_pdf_content_type(ct: str) -> bool:
    return ct and "application/pdf" in ct.lower()


def is_image_content_type(ct: str) -> bool:
    return ct and ct.lower().startswith("image/")


def fetch(session: requests.Session, url: str, stream: bool = True, timeout: int = 30) -> requests.Response:
    r = session.get(url, stream=stream, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r


def download_pdf(session: requests.Session, url: str, dest: Path) -> bool:
    try:
        r = fetch(session, url, stream=True)
        ct = r.headers.get("Content-Type", "").lower()
        # Si el servidor no pone bien el CT pero la URL acaba en .pdf, aceptamos igualmente
        if not (is_pdf_content_type(ct) or url.lower().endswith(".pdf")):
            return False
        with open(dest, "wb") as f:
            for chunk in r.iter_content(65536):
                if chunk:
                    f.write(chunk)
        return True
    except Exception:
        return False


def download_image_as_jpg(session: requests.Session, url: str, dest_jpg: Path) -> bool:
    """
    Descarga imagen (cualquier formato) y la convierte a JPG.
    """
    try:
        r = fetch(session, url, stream=True)
        ct = r.headers.get("Content-Type", "").lower()
        # Aceptamos si el CT es image/* o si la URL parece imagen por extensión conocida
        if not (is_image_content_type(ct) or re.search(r"\.(png|jpg|jpeg|webp|bmp|gif|tif|tiff)$", url, re.I)):
            return False

        data = io.BytesIO(r.content)
        im = Image.open(data)
        # Convertir a RGB si es RGBA/L o similar
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        elif im.mode == "L":
            im = im.convert("RGB")
        ensure_dir(dest_jpg.parent)
        im.save(dest_jpg, format="JPEG", quality=90, optimize=True)
        return True
    except Exception:
        return False


def zip_dir(root_dir: Path, zip_path: Path):
    with ZipFile(zip_path, "w", ZIP_DEFLATED) as zf:
        for p in root_dir.rglob("*"):
            if p.is_file():
                zf.write(p, p.relative_to(root_dir))


# -------------------- Main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="data/RESUMEN_CATALOGO_READY.xlsx",
                    help="Ruta del Excel enriquecido (por defecto: data/RESUMEN_CATALOGO_READY.xlsx)")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                    help=f"Directorio raíz de salida (por defecto: {DEFAULT_OUT_DIR})")
    ap.add_argument("--zip-name", default=DEFAULT_ZIP,
                    help=f"Nombre del ZIP a generar (por defecto: {DEFAULT_ZIP})")
    ap.add_argument("--provider-contains", default="",
                    help='Descargar SOLO filas cuyo "Proveedor" contenga este texto (normalizado). Vacío = todas')
    ap.add_argument("--overwrite", action="store_true",
                    help="Sobrescribir ficheros existentes (por defecto no)")
    args = ap.parse_args()

    excel_path = Path(args.excel)
    out_root   = Path(args.out_dir)
    zip_path   = Path(args.zip_name)
    prov_filter = norm_text(args.provider_contains)

    if not excel_path.exists():
        print(f"[ERROR] Excel no encontrado: {excel_path}")
        raise SystemExit(1)

    df = pd.read_excel(excel_path, sheet_name=0)

    # Comprobar columnas
    for key in COLS.values():
        if key not in df.columns:
            print(f"[ERROR] Falta columna en el Excel: '{key}'")
            raise SystemExit(1)

    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})

    ensure_dir(out_root)
    imgs_root  = out_root / "IMAGENES"
    pdfs_root  = out_root / "FICHAS"
    ensure_dir(imgs_root)
    ensure_dir(pdfs_root)

    total = len(df)
    stats = {"rows": total, "skipped_excluded": 0, "skipped_provider": 0,
             "img_ok": 0, "img_skip": 0, "pdf_ok": 0, "pdf_skip": 0}

    for i, row in df.iterrows():
        cod_art  = row[COLS["cod_art"]]
        refprov  = str(row[COLS["refprov"]] if not is_empty(row[COLS["refprov"]]) else "").strip()
        prov     = str(row[COLS["prov"]] if not is_empty(row[COLS["prov"]]) else "").strip()
        codprov  = str(row[COLS["codprov"]] if not is_empty(row[COLS["codprov"]]) else "").strip()
        url_img  = str(row[COLS["img"]] if not is_empty(row[COLS["img"]]) else "").strip()
        url_pdf  = str(row[COLS["pdf"]] if not is_empty(row[COLS["pdf"]]) else "").strip()

        # 1) Excluir FAMARA
        if norm_text(prov) and any(ex in norm_text(prov) for ex in (norm_text(x) for x in EXCLUDE_PROVIDERS)):
            stats["skipped_excluded"] += 1
            continue

        # 2) Filtrar por proveedor literal si se ha indicado
        if prov_filter and prov_filter not in norm_text(prov):
            stats["skipped_provider"] += 1
            continue

        # 3) Carpeta por fabricante: "<Cód. Proveedor - Proveedor>"
        folder_name = safe_name(f"{codprov} - {prov}") if (codprov or prov) else "SIN_PROVEEDOR"
        img_dir = imgs_root / folder_name
        pdf_dir = pdfs_root / folder_name
        ensure_dir(img_dir); ensure_dir(pdf_dir)

        # 4) Nombre base: "<Cód. Articulo Naves - Referencia Proveedor>"
        base_name = safe_name(f"{cod_art} - {refprov}".strip())
        if not base_name:
            base_name = f"fila_{i+1}"

        # 5) Descargar imagen (si hay URL)
        if url_img:
            img_dest = img_dir / f"{base_name}.jpg"
            if img_dest.exists() and not args.overwrite:
                stats["img_skip"] += 1
            else:
                ok = download_image_as_jpg(s, url_img, img_dest)
                if ok:
                    stats["img_ok"] += 1
                else:
                    stats["img_skip"] += 1

        # 6) Descargar PDF (si hay URL)
        if url_pdf:
            pdf_dest = pdf_dir / f"{base_name}.pdf"
            if pdf_dest.exists() and not args.overwrite:
                stats["pdf_skip"] += 1
            else:
                ok = download_pdf(s, url_pdf, pdf_dest)
                if ok:
                    stats["pdf_ok"] += 1
                else:
                    stats["pdf_skip"] += 1

    # 7) Crear ZIP
    if zip_path.exists():
        zip_path.unlink()
    if any(out_root.rglob("*")):
        zip_dir(out_root, zip_path)

    print("[OK] Proceso finalizado.")
    print("[OK] Estadísticas:", stats)
    print(f"[OK] Carpeta: {out_root}  |  ZIP: {zip_path}  (si había contenido)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
