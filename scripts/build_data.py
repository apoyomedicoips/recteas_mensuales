# scripts/build_data.py
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


def _norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_csv_flexible(fp: Path) -> pd.DataFrame:
    # intenta delimitadores y encodings frecuentes
    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(fp, dtype=str, encoding=enc)
        except Exception:
            pass
        try:
            return pd.read_csv(fp, dtype=str, encoding=enc, sep=";")
        except Exception:
            pass
    return pd.read_csv(fp, dtype=str, encoding="latin1")


def _coerce_numeric(s: pd.Series) -> pd.Series:
    x = (
        s.astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^0-9\.\-]", "", regex=True)
    )
    return pd.to_numeric(x, errors="coerce")


def _clean_code_str(x: pd.Series) -> pd.Series:
    # limpia códigos que vienen como float (ej: ".6622400000000001" o "27491.0")
    s = x.astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)
    # si empieza con ".", prefija 0 para parsear
    s = s.str.replace(r"^\.", "0.", regex=True)
    n = pd.to_numeric(s, errors="coerce")
    out = n.fillna(np.nan)
    out = out.map(lambda v: "" if pd.isna(v) else str(int(v)))
    # para casos no numéricos, conserva el string original saneado
    out = out.where(out != "", s)
    out = out.str.replace(r"\.0+$", "", regex=True)
    return out


def _find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    up = {c.upper(): c for c in cols}
    for cand in candidates:
        c = cand.upper()
        if c in up:
            return up[c]
    return None


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
    "COD_PROD": ["COD_PRODUCTO", "COD_PROD", "COD_MEDICAMENTO", "CODIGO_PRODUCTO", "COD_ITEM", "ITEM", "COD", "MAXCOD"],
    "CANTIDAD": ["CANTIDAD", "CANT", "QTY", "CANT_SOL", "CANT_ENT", "CANT_DISP", "CANTIDAD_ENTREGADA"],
    "ALMACEN": ["ALMACEN", "DEPOSITO", "FARMACIA", "BODEGA", "ALM", "ALMACEN_ORIGEN", "ALMACEN_DESTINO", "COD_ALMACEN", "ALMACEN_CODIGO"],
    "SERVICIO": ["SERVICIO", "DEPENDENCIA", "UNIDAD", "SECTOR", "AREA", "SALA"],
    "COD_MEDICO": ["COD_MEDICO", "COD_PROF", "COD_PRESCRIPTOR", "ID_MEDICO", "MEDICO_COD", "CÓDIGO_DEL_MÉDICO", "CODIGO_DEL_MEDICO"],
    "ID_PACIENTE": ["CEDULA", "CI", "NRO_DOC", "DOCUMENTO", "PACIENTE_CI", "ID_PACIENTE", "CÉDULAPACIENTE", "CEDULAPACIENTE"],
    "DESC_PROD": ["DESC_PRODUCTO", "DESCRIPCION", "PRODUCTO", "NOMBRE_PRODUCTO", "ITEM_DESC", "MEDICAMENTO", "TEXTOBREVEMEDICAMENTO"],
}


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


def infer_column_map(df: pd.DataFrame) -> ColumnMap:
    cols = list(df.columns)

    fecha = _find_col(cols, DEFAULT_SYNONYMS["FECHA"])
    cod_prod = _find_col(cols, DEFAULT_SYNONYMS["COD_PROD"])
    cantidad = _find_col(cols, DEFAULT_SYNONYMS["CANTIDAD"])

    if fecha is None or cod_prod is None or cantidad is None:
        missing = [k for k, v in [("FECHA", fecha), ("COD_PROD", cod_prod), ("CANTIDAD", cantidad)] if v is None]
        raise ValueError(f"No se pudieron inferir columnas requeridas: {missing}. Encabezados: {cols}")

    almacen = _find_col(cols, DEFAULT_SYNONYMS["ALMACEN"])
    servicio = _find_col(cols, DEFAULT_SYNONYMS["SERVICIO"])
    cod_medico = _find_col(cols, DEFAULT_SYNONYMS["COD_MEDICO"])
    id_paciente = _find_col(cols, DEFAULT_SYNONYMS["ID_PACIENTE"])
    desc_prod = _find_col(cols, DEFAULT_SYNONYMS["DESC_PROD"])

    return ColumnMap(fecha, cod_prod, cantidad, almacen, servicio, cod_medico, id_paciente, desc_prod)


def build_lookups(repo_dir: Path) -> Dict[str, Dict[str, str]]:
    out = {"prod": {}, "med": {}, "pac": {}, "alm": {}}

    # Productos (maxcod -> TextoBreveMedicamento)
    fp = repo_dir / "recetas2025_codigosproductos.csv"
    if fp.exists():
        df = _read_csv_flexible(fp)
        if {"maxcod", "TextoBreveMedicamento"}.issubset(df.columns):
            key = df["maxcod"].astype(str).str.strip()
            val = df["TextoBreveMedicamento"].astype(str).str.strip()
            out["prod"] = dict(zip(key, val))

    # Médicos (CódigodelMédico -> NombredelMédico)
    fp = repo_dir / "recetas2025_codigosmedicos.csv"
    if fp.exists():
        df = _read_csv_flexible(fp)
        if {"CódigodelMédico", "NombredelMédico"}.issubset(df.columns):
            key = _clean_code_str(df["CódigodelMédico"])
            val = df["NombredelMédico"].astype(str).str.strip()
            out["med"] = dict(zip(key, val))

    # Pacientes (CédulaPaciente -> NombreyApellido)
    fp = repo_dir / "recetas2025_cedulaspacientes.csv"
    if fp.exists():
        df = _read_csv_flexible(fp)
        if {"CédulaPaciente", "NombreyApellido"}.issubset(df.columns):
            key = _clean_code_str(df["CédulaPaciente"])
            val = df["NombreyApellido"].astype(str).str.strip()
            out["pac"] = dict(zip(key, val))

    # Recurrentes (CédulaRecurrente -> NombreRecurrente)
    fp = repo_dir / "recetas2025_cedularecurrentes.csv"
    if fp.exists():
        df = _read_csv_flexible(fp)
        if {"CédulaRecurrente", "NombreRecurrente"}.issubset(df.columns):
            key = _clean_code_str(df["CédulaRecurrente"])
            val = df["NombreRecurrente"].astype(str).str.strip()
            # solo completa faltantes del lookup principal
            for k, v in dict(zip(key, val)).items():
                if k and (k not in out["pac"] or not out["pac"][k]):
                    out["pac"][k] = v

    # Almacenes: viene como una sola columna "almacen_codigo;almacen_descripcion"
    fp = repo_dir / "recetas2025_codigosalmacenes.csv"
    if fp.exists():
        df = _read_csv_flexible(fp)
        if len(df.columns) == 1 and ";" in df.columns[0]:
            # reconstruye columnas
            col = df.columns[0]
            tmp = df[col].astype(str).str.split(";", n=1, expand=True)
            tmp.columns = ["almacen_codigo", "almacen_descripcion"]
            key = _clean_code_str(tmp["almacen_codigo"])
            val = tmp["almacen_descripcion"].astype(str).str.strip()
            out["alm"] = dict(zip(key, val))
        elif {"almacen_codigo", "almacen_descripcion"}.issubset(df.columns):
            key = _clean_code_str(df["almacen_codigo"])
            val = df["almacen_descripcion"].astype(str).str.strip()
            out["alm"] = dict(zip(key, val))

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-dir", type=str, default=".")
    ap.add_argument("--input-dir", type=str, default=".")
    ap.add_argument("--parquet-glob", type=str, default="*.parquet")
    ap.add_argument("--out-dir", type=str, default="docs/data")
    ap.add_argument("--freq", type=str, default="D", choices=["D", "M"])
    args = ap.parse_args()

    repo_dir = Path(args.repo_dir).resolve()
    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    _ensure_dir(out_dir)

    parquet_files = sorted([p for p in input_dir.glob(args.parquet_glob) if p.is_file()])
    if not parquet_files:
        raise FileNotFoundError(f"No se encontraron parquet con patrón {args.parquet_glob} en {input_dir}")

    lookups = build_lookups(repo_dir)
    prod_lookup = lookups["prod"]
    med_lookup = lookups["med"]
    pac_lookup = lookups["pac"]
    alm_lookup = lookups["alm"]

    dfs = []
    schema_samples = []

    for fp in parquet_files:
        df = pd.read_parquet(fp)  # requiere pyarrow instalado localmente
        schema_samples.append({"file": fp.name, "columns": list(df.columns)})

        cmap = infer_column_map(df)

        keep = [cmap.fecha, cmap.cod_prod, cmap.cantidad]
        for opt in [cmap.almacen, cmap.servicio, cmap.cod_medico, cmap.id_paciente, cmap.desc_prod]:
            if opt is not None:
                keep.append(opt)

        d = df[keep].copy()

        ren = {
            cmap.fecha: "FECHA",
            cmap.cod_prod: "COD_PROD",
            cmap.cantidad: "CANTIDAD",
        }
        if cmap.almacen is not None:
            ren[cmap.almacen] = "ALMACEN"
        if cmap.servicio is not None:
            ren[cmap.servicio] = "SERVICIO"
        if cmap.cod_medico is not None:
            ren[cmap.cod_medico] = "COD_MEDICO"
        if cmap.id_paciente is not None:
            ren[cmap.id_paciente] = "ID_PACIENTE"
        if cmap.desc_prod is not None:
            ren[cmap.desc_prod] = "DESC_PROD"

        d.rename(columns=ren, inplace=True)

        for col in ["ALMACEN", "SERVICIO", "COD_MEDICO", "ID_PACIENTE", "DESC_PROD"]:
            if col not in d.columns:
                d[col] = None

        d["FECHA"] = pd.to_datetime(d["FECHA"], errors="coerce")
        d["CANTIDAD"] = _coerce_numeric(d["CANTIDAD"])
        d["COD_PROD"] = _clean_code_str(d["COD_PROD"])
        d["COD_MEDICO"] = _clean_code_str(d["COD_MEDICO"])
        d["ID_PACIENTE"] = _clean_code_str(d["ID_PACIENTE"])
        d["ALMACEN"] = _clean_code_str(d["ALMACEN"])

        # Enriquecimientos
        d["NOMBRE_PROD"] = d["COD_PROD"].map(prod_lookup)
        d["NOMBRE_PROD"] = d["NOMBRE_PROD"].fillna(d["DESC_PROD"].astype(str))
        d["NOMBRE_PROD_NORM"] = d["NOMBRE_PROD"].map(_norm_text)
        d["ITEM_CRITICO"] = d["NOMBRE_PROD_NORM"].map(_classify_item)

        d["MEDICO"] = d["COD_MEDICO"].map(med_lookup).fillna(d["COD_MEDICO"].astype(str))
        d["PACIENTE"] = d["ID_PACIENTE"].map(pac_lookup).fillna(d["ID_PACIENTE"].astype(str))
        d["ALMACEN_DESC"] = d["ALMACEN"].map(alm_lookup).fillna(d["ALMACEN"].astype(str))

        d = d.dropna(subset=["FECHA"])
        d = d[d["ITEM_CRITICO"].notna()].copy()

        d["SERVICIO"] = d["SERVICIO"].astype(str).replace({"nan": "", "None": ""})
        d["ALMACEN_DESC"] = d["ALMACEN_DESC"].astype(str).replace({"nan": "", "None": ""})

        dfs.append(d)

    data = pd.concat(dfs, ignore_index=True)

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

    def agg_dim(dim: str) -> pd.DataFrame:
        x = (
            data.groupby(["ITEM_CRITICO", dim], as_index=False)["CANTIDAD"]
            .sum()
            .rename(columns={"CANTIDAD": "Q"})
            .sort_values(["ITEM_CRITICO", "Q"], ascending=[True, False])
        )
        return x

    agg_almacen = agg_dim("ALMACEN_DESC").rename(columns={"ALMACEN_DESC": "ALMACEN"})
    agg_servicio = agg_dim("SERVICIO")
    agg_medico = agg_dim("MEDICO")

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

    schema_report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "parquet_glob": args.parquet_glob,
        "files": schema_samples,
        "note": "Si no aparecen datos, revise en schema_report.json los encabezados reales del parquet y ajuste DEFAULT_SYNONYMS.",
        "n_rows_filtered_critical": int(len(data)),
        "n_items": int(items.shape[0]),
    }

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
