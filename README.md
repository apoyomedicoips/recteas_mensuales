# Tablero de Stock Crítico (Recetas Mensuales)

## 1. Generación de datos (ETL)
Requisitos:
- Python 3.10+
- pip install -r scripts/requirements.txt

Ejecución (desde la raíz del repo):
- python scripts/build_data.py --repo-dir . --input-dir . --parquet-glob "*.parquet" --out-dir docs/data --freq D

Esto genera JSON en:
- docs/data

## 2. Publicación GitHub Pages
- Settings -> Pages
- Source: Deploy from a branch
- Branch: main
- Folder: /docs

Abrir el enlace que provee GitHub Pages.

## 3. Notas sobre login
En GitHub Pages el login es un control de acceso en el navegador (hash), no es seguridad real si el repo es público.
Para seguridad real use repositorio privado con acceso controlado o un servicio de autenticación (por ejemplo, Cloudflare Access, Firebase Auth, o un backend).
