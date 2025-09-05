# Catálogo automático (imágenes + fichas) vía GitHub Actions

Este repositorio genera la estructura exigida:
```
CATALOGO/
  IMAGENES/<Cód. Proveedor - Proveedor>/<Cód. Articulo Naves - Referencia Proveedor>.jpg
  FICHAS/<Cód. Proveedor - Proveedor>/<Cód. Articulo Naves - Referencia Proveedor>.pdf
```
y un `CATALOGO.zip`, todo de forma **automática** en GitHub Actions.

## Uso rápido (3 pasos)
1. Crea un **repo privado** en tu GitHub.
2. Sube el contenido de esta carpeta (arrastrar y soltar desde el navegador).
3. Coloca tu Excel en `data/RESUMEN_CATALOGO.xlsx` (mismo formato que el template). Haz un commit.

La acción **se ejecuta sola** y verás en la pestaña **Actions**:
- Un **artifact** llamado `CATALOGO_ZIP` con el `CATALOGO.zip` listo para descargar.
- El árbol `CATALOGO/` dentro de `output/` en el propio repo.

## Formato del Excel (hoja 1)
Encabezados obligatorios (exactos o muy similares):
- `Cód. Articulo Naves`
- `Referencia Proveedor`
- `Artículo`
- `Proveedor`
- `Cód. Proveedor`
- `URL Imagen Oficial`
- `URL Ficha Técnica Oficial`

> Solo se descargan las filas que **tengan URL**. Si una URL de ficha no es PDF, **se omite** por seguridad.

## Arranque manual
Si quieres disparar el proceso sin hacer commit: pestaña **Actions → Build Catalog → Run workflow**.
