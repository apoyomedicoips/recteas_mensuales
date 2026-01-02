# scripts/build_data.py
from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
# Utilidades
# =========================
def _norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _safe_to_datetime(x) -> pd.Series:
    return pd.to_datetime(x, errors="coerce")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_csv_any_encoding(fp: Path) -> pd.DataFrame:
    # Intentos típicos para CSVs locales
    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(fp, dtype=str, encoding=enc)
        except Exception:
            pass
    # último intento
    return pd.read_csv(fp, dtype=str, encoding="latin1")


def _find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    up = {c.upper(): c for c in cols}
    for cand in candidates:
        c = cand.upper()
        if c in up:
            return up[c]
    return None


def _coerce_numeric(s: pd.Series) -> pd.Series:
    x = (
        s.astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^0-9\.\-]", "", regex=True)
    )
    return pd.to_numeric(x, errors="coerce")


# =========================
# Configuración de mapeo
# =========================
@dataclass(frozen=True)
class ColumnMap:
    fecha: str
    cod_prod: str
    cantidad: str
    almacen: Optional[str]
    servicio: Optional[str]
    cod_medico: Optional[str]
    id_paciente: Optional[str]
    desc_prod: Optional[str]


DEFAULT_SYNONYMS = {
    "FECHA": ["FECHA", "FECHA_RECETA", "FEC_RECETA", "FEC_EMI", "FECHA_EMISION", "DATE"],
    "COD_PROD": ["COD_PRODUCTO", "COD_PROD", "COD_MEDICAMENTO", "CODIGO_PRODUCTO", "COD_ITEM", "ITEM", "COD"],
    "CANTIDAD": ["CANTIDAD", "CANT", "QTY", "CANT_SOL", "CANT_ENT", "CANT_DISP", "CANTIDAD_ENTREGADA"],
    "ALMACEN": ["ALMACEN", "DEPOSITO", "FARMACIA", "BODEGA", "ALM", "ALMACEN_ORIGEN", "ALMACEN_DESTINO"],
    "SERVICIO": ["SERVICIO", "DEPENDENCIA", "UNIDAD", "SECTOR", "AREA", "SALA"],
    "COD_MEDICO": ["COD_MEDICO", "COD_PROF", "COD_PRESCRIPTOR", "ID_MEDICO", "MEDICO_COD"],
    "ID_PACIENTE": ["CEDULA", "CI", "NRO_DOC", "DOCUMENTO", "PACIENTE_CI", "ID_PACIENTE"],
    "DESC_PROD": ["DESC_PRODUCTO", "DESCRIPCION", "PRODUCTO", "NOMBRE_PRODUCTO", "ITEM_DESC", "MEDICAMENTO"],
}


# =========================
# Lista crítica, reglas de matching
# =========================
CRITICAL_ITEMS = [
    "Alcohol",
    "Guantes",
    "Solucion fisiologica",
    "IOP solucion",
    "IOP jabon",
    "Jeringas de 5ml",
    "jeringas de 10ml",
    "algodon",
    "macrogotero",
    "microgotero",
    "volutrol",
    "clorhexidina",
    "punzocath 20",
    "punzocath 22",
    "punzocath 18",
]

# Reglas basadas en nombre (texto normalizado)
CRITICAL_PATTERNS = {
    "ALCOHOL": [r"\balcohol\b"],
    "GUANTES": [r"\bguante(s)?\b"],
    "SOLUCION FISIOLOGICA": [r"\bsolucion\b.*\bfisiologic(a|o)\b", r"\bfisiologic(a|o)\b"],
    "IOP SOLUCION": [r"\biop\b.*\bsolucion\b", r"\b(iop)\b.*\bsol\b"],
    "IOP JABON": [r"\biop\b.*\bjabon\b", r"\biop\b.*\bsoap\b"],
    "JERINGA 5ML": [r"\bjeringa(s)?\b.*\b5\b\s*ml\b", r"\b5\s*ml\b.*\bjeringa\b"],
    "JERINGA 10ML": [r"\bjeringa(s)?\b.*\b10\b\s*ml\b", r"\b10\s*ml\b.*\bjeringa\b"],
    "ALGODON": [r"\balgodon\b"],
    "MACROGOTERO": [r"\bmacrogotero\b", r"\bmacro\b.*\bgote(ro)?\b"],
    "MICROGOTERO": [r"\bmicrogotero\b", r"\bmicro\b.*\bgote(ro)?\b"],
    "VOLUTROL": [r"\bvolutrol\b"],
    "CLORHEXIDINA": [r"\bclorhexidina\b", r"\bchlorhexidine\b"],
    "PUNZOCATH 18": [r"\bpunzocath\b.*\b18\b", r"\bpunzo\b.*\b18\b"],
    "PUNZOCATH 20": [r"\bpunzocath\b.*\b20\b", r"\bpunzo\b.*\b20\b"],
    "PUNZOCATH 22": [r"\bpunzocath\b.*\b22\b", r"\bpunzo\b.*\b22\b"],
}


def _classify_item(product_name_norm: str) -> Optional[str]:
    for label, pats in CRITICAL_PATTERNS.items():
        for pat in pats:
            if re.search(pat, product_name_norm):
                return label
    return None


# =========================
# Lectura y mapeo de columnas
# =========================
def infer_column_map(df: pd.DataFrame, synonyms: Dict[str, List[str]]) -> ColumnMap:
    cols = list(df.columns)

    fecha = _find_col(cols, synonyms["FECHA"])
    cod_prod = _find_col(cols, synonyms["COD_PROD"])
    cantidad = _find_col(cols, synonyms["CANTIDAD"])

    if fecha is None or cod_prod is None or cantidad is None:
        missing = [k for k, v in [("FECHA", fecha), ("COD_PROD", cod_prod), ("CANTIDAD", cantidad)] if v is None]
        raise ValueError(f"No se pudieron inferir columnas requeridas: {missing}. Encabezados encontrados: {cols}")

    almacen = _find_col(cols, synonyms["ALMACEN"])
    servicio = _find_col(cols, synonyms["SERVICIO"])
    cod_medico = _find_col(cols, synonyms["COD_MEDICO"])
    id_paciente = _find_col(cols, synonyms["ID_PACIENTE"])
    desc_prod = _find_col(cols, synonyms["DESC_PROD"])

    return ColumnMap(
        fecha=fecha,
        cod_prod=cod_prod,
        cantidad=cantidad,
        almacen=almacen,
        servicio=servicio,
        cod_medico=cod_medico,
        id_paciente=id_paciente,
        desc_prod=desc_prod,
    )


def read_parquet_files(input_dir: Path, pattern: str) -> List[Path]:
    fps = sorted(input_dir.glob(pattern))
    return [p for p in fps if p.is_file()]


def read_references(repo_dir: Path) -> Dict[str, pd.DataFrame]:
    refs = {}
    for name in [
        "recetas2025_codigosproductos.csv",
        "recetas2025_codigosmedicos.csv",
        "recetas2025_cedulapacientes.csv",
        "recetas2025_cedularecurrentes.csv",
    ]:
        fp = repo_dir / name
        if fp.exists():
            refs[name] = _read_csv_any_encoding(fp)
    return refs


def _guess_key_value_cols(df: pd.DataFrame) -> Tuple[str, str]:
    cols = list(df.columns)
    if len(cols) < 2:
        raise ValueError("Referencia CSV sin al menos 2 columnas (clave, valor).")
    # Heurística: primera columna como clave, segunda como valor
    return cols[0], cols[1]


def build_lookup(df_ref: pd.DataFrame) -> Dict[str, str]:
    key_col, val_col = _guess_key_value_cols(df_ref)
    tmp = df_ref[[key_col, val_col]].copy()
    tmp[key_col] = tmp[key_col].astype(str).str.strip()
    tmp[val_col] = tmp[val_col].astype(str).str.strip()
    tmp = tmp.dropna()
    tmp = tmp[tmp[key_col] != ""]
    return dict(zip(tmp[key_col], tmp[val_col]))


# =========================
# ETL principal
# =========================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-dir", type=str, default=".", help="Directorio raíz del repo")
    ap.add_argument("--input-dir", type=str, default=".", help="Directorio donde están los parquet")
    ap.add_argument("--parquet-glob", type=str, default="*.parquet", help="Patrón glob de parquet")
    ap.add_argument("--out-dir", type=str, default="docs/data", help="Salida JSON para GitHub Pages")
    ap.add_argument("--freq", type=str, default="D", choices=["D", "M"], help="Frecuencia (D diario, M mensual)")
    args = ap.parse_args()

    repo_dir = Path(args.repo_dir).resolve()
    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    _ensure_dir(out_dir)

    parquet_files = read_parquet_files(input_dir, args.parquet_glob)
    if len(parquet_files) == 0:
        raise FileNotFoundError(f"No se encontraron parquet con patrón {args.parquet_glob} en {input_dir}")

    refs = read_references(repo_dir)

    prod_lookup = {}
    med_lookup = {}
    pac_lookup = {}

    if "recetas2025_codigosproductos.csv" in refs:
        prod_lookup = build_lookup(refs["recetas2025_codigosproductos.csv"])
    if "recetas2025_codigosmedicos.csv" in refs:
        med_lookup = build_lookup(refs["recetas2025_codigosmedicos.csv"])
    # Se prioriza cedulapacientes si existe, sino cedularecurrentes
    if "recetas2025_cedulapacientes.csv" in refs:
        pac_lookup = build_lookup(refs["recetas2025_cedulapacientes.csv"])
    elif "recetas2025_cedularecurrentes.csv" in refs:
        pac_lookup = build_lookup(refs["recetas2025_cedularecurrentes.csv"])

    # Lectura y concatenación
    dfs = []
    schema_samples = []

    for fp in parquet_files:
        df = pd.read_parquet(fp)  # requiere pyarrow
        schema_samples.append({"file": fp.name, "columns": list(df.columns)})
        cmap = infer_column_map(df, DEFAULT_SYNONYMS)

        # Subset mínimo
        keep = [cmap.fecha, cmap.cod_prod, cmap.cantidad]
        for opt in [cmap.almacen, cmap.servicio, cmap.cod_medico, cmap.id_paciente, cmap.desc_prod]:
            if opt is not None:
                keep.append(opt)

        d = df[keep].copy()

        d.rename(
            columns={
                cmap.fecha: "FECHA",
                cmap.cod_prod: "COD_PROD",
                cmap.cantidad: "CANTIDAD",
                (cmap.almacen or ""): "ALMACEN",
                (cmap.servicio or ""): "SERVICIO",
                (cmap.cod_medico or ""): "COD_MEDICO",
                (cmap.id_paciente or ""): "ID_PACIENTE",
                (cmap.desc_prod or ""): "DESC_PROD",
            },
            inplace=True,
        )

        # Si algún opcional no existe, puede quedar columna vacía
        for col in ["ALMACEN", "SERVICIO", "COD_MEDICO", "ID_PACIENTE", "DESC_PROD"]:
            if col not in d.columns:
                d[col] = pd.Series([None] * len(d), index=d.index)

        d["FECHA"] = _safe_to_datetime(d["FECHA"])
        d["CANTIDAD"] = _coerce_numeric(d["CANTIDAD"])

        # Nombre de producto por referencia o por descripción interna
        d["NOMBRE_PROD"] = d["COD_PROD"].astype(str).map(prod_lookup)
        # fallback a DESC_PROD si NOMBRE_PROD es NA
        d["NOMBRE_PROD"] = d["NOMBRE_PROD"].fillna(d["DESC_PROD"].astype(str))

        # Normalización y clasificación
        d["NOMBRE_PROD_NORM"] = d["NOMBRE_PROD"].astype(str).map(_norm_text)
        d["ITEM_CRITICO"] = d["NOMBRE_PROD_NORM"].map(_classify_item)

        # Uniones complementarias (solo etiquetas)
        if len(med_lookup) > 0:
            d["MEDICO"] = d["COD_MEDICO"].astype(str).map(med_lookup)
        else:
            d["MEDICO"] = d["COD_MEDICO"].astype(str)

        if len(pac_lookup) > 0:
            d["PACIENTE"] = d["ID_PACIENTE"].astype(str).map(pac_lookup)
        else:
            d["PACIENTE"] = d["ID_PACIENTE"].astype(str)

        # Filtrado por críticos
        d = d[d["ITEM_CRITICO"].notna()].copy()

        # Limpieza final
        d = d.dropna(subset=["FECHA"])
        d["ALMACEN"] = d["ALMACEN"].astype(str).replace({"nan": "", "None": ""})
        d["SERVICIO"] = d["SERVICIO"].astype(str).replace({"nan": "", "None": ""})
        d["MEDICO"] = d["MEDICO"].astype(str).replace({"nan": "", "None": ""})
        d["PACIENTE"] = d["PACIENTE"].astype(str).replace({"nan": "", "None": ""})

        dfs.append(d)

    data = pd.concat(dfs, ignore_index=True)

    # Agregados de series
    data["FECHA_DIA"] = data["FECHA"].dt.floor("D")
    if args.freq == "M":
        data["PERIODO"] = data["FECHA_DIA"].dt.to_period("M").dt.to_timestamp()
    else:
        data["PERIODO"] = data["FECHA_DIA"]

    series = (
        data.groupby(["PERIODO", "ITEM_CRITICO"], as_index=False)["CANTIDAD"]
        .sum()
        .rename(columns={"CANTIDAD": "Q"})
        .sort_values(["PERIODO", "ITEM_CRITICO"])
    )

    # Agregados por dimensión principales
    def agg_dim(dim: str) -> pd.DataFrame:
        x = (
            data.groupby(["ITEM_CRITICO", dim], as_index=False)["CANTIDAD"]
            .sum()
            .rename(columns={"CANTIDAD": "Q"})
            .sort_values(["ITEM_CRITICO", "Q"], ascending=[True, False])
        )
        return x

    agg_almacen = agg_dim("ALMACEN")
    agg_servicio = agg_dim("SERVICIO")
    agg_medico = agg_dim("MEDICO")

    # KPIs por item
    today_max = data["FECHA_DIA"].max()
    last7_start = today_max - pd.Timedelta(days=6)
    last30_start = today_max - pd.Timedelta(days=29)

    kpi = (
        data.groupby("ITEM_CRITICO", as_index=False)["CANTIDAD"]
        .sum()
        .rename(columns={"CANTIDAD": "Q_TOTAL"})
    )

    last7 = (
        data[data["FECHA_DIA"].between(last7_start, today_max)]
        .groupby("ITEM_CRITICO", as_index=False)["CANTIDAD"]
        .sum()
        .rename(columns={"CANTIDAD": "Q_7D"})
    )

    last30 = (
        data[data["FECHA_DIA"].between(last30_start, today_max)]
        .groupby("ITEM_CRITICO", as_index=False)["CANTIDAD"]
        .sum()
        .rename(columns={"CANTIDAD": "Q_30D"})
    )

    kpi = kpi.merge(last7, on="ITEM_CRITICO", how="left").merge(last30, on="ITEM_CRITICO", how="left")
    kpi["Q_7D"] = kpi["Q_7D"].fillna(0.0)
    kpi["Q_30D"] = kpi["Q_30D"].fillna(0.0)
    kpi["AVG_DAILY_30D"] = kpi["Q_30D"] / 30.0
    kpi = kpi.sort_values("Q_TOTAL", ascending=False)

    items = pd.DataFrame({"ITEM_CRITICO": sorted(kpi["ITEM_CRITICO"].unique().tolist())})

    # Reporte de esquema
    schema_report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "parquet_glob": args.parquet_glob,
        "files": schema_samples,
        "inferred_columns_note": "El mapeo se infiere por sinónimos, si falta algún campo, ajuste DEFAULT_SYNONYMS.",
        "critical_items_input": CRITICAL_ITEMS,
        "critical_items_labels": sorted(list(CRITICAL_PATTERNS.keys())),
    }

    # Guardado JSON
    (out_dir / "schema_report.json").write_text(json.dumps(schema_report, ensure_ascii=False, indent=2), encoding="utf-8")
    items.to_json(out_dir / "items.json", orient="records", force_ascii=False)
    kpi.to_json(out_dir / "kpi.json", orient="records", force_ascii=False)
    series.to_json(out_dir / "series_day.json", orient="records", force_ascii=False)

    agg_payload = {
        "almacen": agg_almacen.to_dict(orient="records"),
        "servicio": agg_servicio.to_dict(orient="records"),
        "medico": agg_medico.to_dict(orient="records"),
    }
    (out_dir / "agg_dim.json").write_text(json.dumps(agg_payload, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()

