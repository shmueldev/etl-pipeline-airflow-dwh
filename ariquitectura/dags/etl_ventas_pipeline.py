"""
DAG: etl_ventas_pipeline
Descripción: Pipeline ETL completo
  - Task 1 (Extract):   Lee tablas de la BD fuente (PostgreSQL externo)
  - Task 2 (Transform): Limpia y construye dimensiones + tabla de hechos con Pandas
  - Task 3 (Load):      Inserta el modelo estrella en el DWH (PostgreSQL en Docker)

Airflow 3.x  |  Python 3.12
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

# ── Cadenas de conexión leídas desde variables de entorno ─────
def get_source_engine():
    host = os.environ["SOURCE_DB_HOST"]
    port = os.environ["SOURCE_DB_PORT"]
    db   = os.environ["SOURCE_DB_NAME"]
    user = os.environ["SOURCE_DB_USER"]
    pw   = os.environ["SOURCE_DB_PASS"]
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")

def get_dwh_engine():
    host = os.environ["DWH_DB_HOST"]
    port = os.environ["DWH_DB_PORT"]
    db   = os.environ["DWH_DB_NAME"]
    user = os.environ["DWH_DB_USER"]
    pw   = os.environ["DWH_DB_PASS"]
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")


# ══════════════════════════════════════════════════════════════
# DAG
# ══════════════════════════════════════════════════════════════
@dag(
    dag_id="etl_ventas_pipeline",
    description="ETL: PostgreSQL fuente → transformación Pandas → DWH modelo estrella",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "ventas", "dwh"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def etl_ventas_pipeline():

    # ══════════════════════════════════════════════════════════
    # TASK 1 — EXTRACT
    # Lee las tablas fuente y las pasa como JSON al XCom
    # ══════════════════════════════════════════════════════════
    @task()
    def extract() -> dict:
        logger.info("=== TASK 1: EXTRACT ===")
        engine = get_source_engine()

        with engine.connect() as conn:
            # ── Leer tablas fuente ────────────────────────────
            # Cambia estas queries por las de tus tablas reales
            clientes  = pd.read_sql("SELECT * FROM clientes",  conn)
            productos = pd.read_sql("SELECT * FROM productos", conn)
            ventas    = pd.read_sql("SELECT * FROM ventas",    conn)

        logger.info(f"Clientes extraídos:  {len(clientes)}")
        logger.info(f"Productos extraídos: {len(productos)}")
        logger.info(f"Ventas extraídas:    {len(ventas)}")

        # XCom solo acepta tipos serializables → convertimos a JSON
        return {
            "clientes":  clientes.to_json(orient="records", date_format="iso"),
            "productos": productos.to_json(orient="records", date_format="iso"),
            "ventas":    ventas.to_json(orient="records",    date_format="iso"),
        }


    # ══════════════════════════════════════════════════════════
    # TASK 2 — TRANSFORM
    # Limpia los datos y construye las tablas del modelo estrella
    # ══════════════════════════════════════════════════════════
    @task()
    def transform(raw: dict) -> dict:
        logger.info("=== TASK 2: TRANSFORM ===")

        # Reconstruir DataFrames desde XCom
        clientes  = pd.read_json(raw["clientes"],  orient="records")
        productos = pd.read_json(raw["productos"], orient="records")
        ventas    = pd.read_json(raw["ventas"],    orient="records")

        # ── Limpieza general ──────────────────────────────────
        clientes  = clientes.drop_duplicates().dropna(subset=["id"])
        productos = productos.drop_duplicates().dropna(subset=["id"])
        ventas    = ventas.drop_duplicates().dropna(subset=["id", "fecha"])

        ventas["fecha"] = pd.to_datetime(ventas["fecha"])

        # ── dim_cliente ───────────────────────────────────────
        dim_cliente = clientes[[
            "id", "nombre", "email", "ciudad", "pais", "segmento"
        ]].rename(columns={"id": "cliente_id_src"})
        dim_cliente["nombre"] = dim_cliente["nombre"].str.strip().str.title()

        # ── dim_producto ──────────────────────────────────────
        dim_producto = productos[[
            "id", "nombre", "categoria", "subcategoria", "precio_unitario"
        ]].rename(columns={"id": "producto_id_src"})
        dim_producto["precio_unitario"] = pd.to_numeric(
            dim_producto["precio_unitario"], errors="coerce"
        ).fillna(0)

        # ── dim_tiempo ────────────────────────────────────────
        fechas_unicas = ventas["fecha"].dt.date.unique()
        dim_tiempo = pd.DataFrame({"fecha": pd.to_datetime(fechas_unicas)})
        dim_tiempo["anio"]        = dim_tiempo["fecha"].dt.year
        dim_tiempo["mes"]         = dim_tiempo["fecha"].dt.month
        dim_tiempo["dia"]         = dim_tiempo["fecha"].dt.day
        dim_tiempo["trimestre"]   = dim_tiempo["fecha"].dt.quarter
        dim_tiempo["nombre_mes"]  = dim_tiempo["fecha"].dt.strftime("%B")
        dim_tiempo["dia_semana"]  = dim_tiempo["fecha"].dt.strftime("%A")
        dim_tiempo["es_fin_semana"] = dim_tiempo["fecha"].dt.dayofweek >= 5
        dim_tiempo["fecha"]       = dim_tiempo["fecha"].dt.date.astype(str)

        # ── fact_ventas (staging — sin IDs DWH todavía) ──────
        # Los IDs reales del DWH se resuelven en el task Load
        fact_staging = ventas[[
            "id", "fecha", "cliente_id", "producto_id",
            "cantidad", "precio_venta", "descuento"
        ]].rename(columns={"id": "venta_id_src"})

        fact_staging["fecha"]        = fact_staging["fecha"].dt.date.astype(str)
        fact_staging["cantidad"]     = fact_staging["cantidad"].astype(int)
        fact_staging["precio_venta"] = pd.to_numeric(fact_staging["precio_venta"], errors="coerce").fillna(0)
        fact_staging["descuento"]    = pd.to_numeric(fact_staging["descuento"],    errors="coerce").fillna(0)
        fact_staging["total_neto"]   = (
            fact_staging["cantidad"] * fact_staging["precio_venta"]
            * (1 - fact_staging["descuento"] / 100)
        ).round(2)

        logger.info(f"dim_tiempo:    {len(dim_tiempo)} filas")
        logger.info(f"dim_cliente:   {len(dim_cliente)} filas")
        logger.info(f"dim_producto:  {len(dim_producto)} filas")
        logger.info(f"fact_staging:  {len(fact_staging)} filas")

        return {
            "dim_tiempo":   dim_tiempo.to_json(orient="records"),
            "dim_cliente":  dim_cliente.to_json(orient="records"),
            "dim_producto": dim_producto.to_json(orient="records"),
            "fact_staging": fact_staging.to_json(orient="records"),
        }


    # ══════════════════════════════════════════════════════════
    # TASK 3 — LOAD
    # Inserta dimensiones y resuelve FK para la tabla de hechos
    # ══════════════════════════════════════════════════════════
    @task()
    def load(transformed: dict) -> None:
        logger.info("=== TASK 3: LOAD ===")

        dim_tiempo   = pd.read_json(transformed["dim_tiempo"],   orient="records")
        dim_cliente  = pd.read_json(transformed["dim_cliente"],  orient="records")
        dim_producto = pd.read_json(transformed["dim_producto"],  orient="records")
        fact_staging = pd.read_json(transformed["fact_staging"], orient="records")

        engine = get_dwh_engine()

        with engine.begin() as conn:

            # ── Cargar dim_tiempo ─────────────────────────────
            for _, row in dim_tiempo.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_tiempo
                        (fecha, anio, mes, dia, trimestre, nombre_mes, dia_semana, es_fin_semana)
                    VALUES
                        (:fecha, :anio, :mes, :dia, :trimestre, :nombre_mes, :dia_semana, :es_fin_semana)
                    ON CONFLICT (fecha) DO NOTHING
                """), row.to_dict())
            logger.info("dim_tiempo cargada")

            # ── Cargar dim_cliente ────────────────────────────
            for _, row in dim_cliente.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_cliente
                        (cliente_id_src, nombre, email, ciudad, pais, segmento)
                    VALUES
                        (:cliente_id_src, :nombre, :email, :ciudad, :pais, :segmento)
                    ON CONFLICT (cliente_id_src) DO UPDATE SET
                        nombre   = EXCLUDED.nombre,
                        email    = EXCLUDED.email,
                        ciudad   = EXCLUDED.ciudad,
                        pais     = EXCLUDED.pais,
                        segmento = EXCLUDED.segmento
                """), row.to_dict())
            logger.info("dim_cliente cargada")

            # ── Cargar dim_producto ───────────────────────────
            for _, row in dim_producto.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_producto
                        (producto_id_src, nombre, categoria, subcategoria, precio_unitario)
                    VALUES
                        (:producto_id_src, :nombre, :categoria, :subcategoria, :precio_unitario)
                    ON CONFLICT (producto_id_src) DO UPDATE SET
                        nombre          = EXCLUDED.nombre,
                        categoria       = EXCLUDED.categoria,
                        subcategoria    = EXCLUDED.subcategoria,
                        precio_unitario = EXCLUDED.precio_unitario
                """), row.to_dict())
            logger.info("dim_producto cargada")

            # ── Resolver IDs del DWH para fact_ventas ────────
            # Traemos el mapeo src_id → dwh_id de cada dimensión
            tiempo_map   = dict(conn.execute(text(
                "SELECT fecha::text, id_tiempo FROM dim_tiempo"
            )).fetchall())
            cliente_map  = dict(conn.execute(text(
                "SELECT cliente_id_src, id_cliente FROM dim_cliente"
            )).fetchall())
            producto_map = dict(conn.execute(text(
                "SELECT producto_id_src, id_producto FROM dim_producto"
            )).fetchall())

            # ── Cargar fact_ventas ────────────────────────────
            cargadas = 0
            omitidas = 0
            for _, row in fact_staging.iterrows():
                id_tiempo   = tiempo_map.get(str(row["fecha"]))
                id_cliente  = cliente_map.get(int(row["cliente_id"]))
                id_producto = producto_map.get(int(row["producto_id"]))

                if not all([id_tiempo, id_cliente, id_producto]):
                    logger.warning(f"Venta {row['venta_id_src']} omitida — FK no resuelta")
                    omitidas += 1
                    continue

                conn.execute(text("""
                    INSERT INTO fact_ventas
                        (id_tiempo, id_cliente, id_producto,
                         cantidad, precio_venta, descuento, total_neto, venta_id_src)
                    VALUES
                        (:id_tiempo, :id_cliente, :id_producto,
                         :cantidad, :precio_venta, :descuento, :total_neto, :venta_id_src)
                """), {
                    "id_tiempo":    id_tiempo,
                    "id_cliente":   id_cliente,
                    "id_producto":  id_producto,
                    "cantidad":     int(row["cantidad"]),
                    "precio_venta": float(row["precio_venta"]),
                    "descuento":    float(row["descuento"]),
                    "total_neto":   float(row["total_neto"]),
                    "venta_id_src": int(row["venta_id_src"]),
                })
                cargadas += 1

        logger.info(f"fact_ventas → {cargadas} filas cargadas, {omitidas} omitidas")
        logger.info("=== PIPELINE COMPLETADO ===")


    # ── Encadenamiento de tasks ───────────────────────────────
    raw_data       = extract()
    transformed    = transform(raw_data)
    load(transformed)


etl_ventas_pipeline()