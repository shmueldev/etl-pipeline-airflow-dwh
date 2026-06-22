-- ============================================================
-- MODELO ESTRELLA: Ventas
-- Data Warehouse - PostgreSQL
-- ============================================================

-- ── Dimensión Tiempo ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo     SERIAL PRIMARY KEY,
    fecha         DATE        NOT NULL UNIQUE,
    anio          INTEGER     NOT NULL,
    mes           INTEGER     NOT NULL,
    dia           INTEGER     NOT NULL,
    trimestre     INTEGER     NOT NULL,
    nombre_mes    VARCHAR(20) NOT NULL,
    dia_semana    VARCHAR(20) NOT NULL,
    es_fin_semana BOOLEAN     NOT NULL
);

-- ── Dimensión Cliente ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_cliente (
    id_cliente    SERIAL PRIMARY KEY,
    cliente_id_src INTEGER    NOT NULL UNIQUE,  -- id original en la fuente
    nombre        VARCHAR(150) NOT NULL,
    email         VARCHAR(150),
    ciudad        VARCHAR(100),
    pais          VARCHAR(100),
    segmento      VARCHAR(50)
);

-- ── Dimensión Producto ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_producto (
    id_producto    SERIAL PRIMARY KEY,
    producto_id_src INTEGER   NOT NULL UNIQUE,  -- id original en la fuente
    nombre         VARCHAR(150) NOT NULL,
    categoria      VARCHAR(100),
    subcategoria   VARCHAR(100),
    precio_unitario NUMERIC(12,2)
);

-- ── Tabla de Hechos: Ventas ───────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_ventas (
    id_venta      SERIAL PRIMARY KEY,
    id_tiempo     INTEGER NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_cliente    INTEGER NOT NULL REFERENCES dim_cliente(id_cliente),
    id_producto   INTEGER NOT NULL REFERENCES dim_producto(id_producto),
    -- métricas
    cantidad      INTEGER      NOT NULL,
    precio_venta  NUMERIC(12,2) NOT NULL,
    descuento     NUMERIC(5,2)  DEFAULT 0,
    total_neto    NUMERIC(14,2) NOT NULL,
    -- trazabilidad
    venta_id_src  INTEGER,   -- id original en la fuente
    cargado_en    TIMESTAMP DEFAULT NOW()
);

-- ── Índices para mejorar performance en Power BI ─────────────
CREATE INDEX IF NOT EXISTS idx_fact_ventas_tiempo    ON fact_ventas(id_tiempo);
CREATE INDEX IF NOT EXISTS idx_fact_ventas_cliente   ON fact_ventas(id_cliente);
CREATE INDEX IF NOT EXISTS idx_fact_ventas_producto  ON fact_ventas(id_producto);