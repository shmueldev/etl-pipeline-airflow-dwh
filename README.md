# Pipeline ETL — Airflow + Docker + PostgreSQL + Power BI

Pipeline ETL completo orquestado con **Apache Airflow 3.2.2**, dockerizado con **Docker Compose**, que extrae datos desde una base de datos PostgreSQL externa, los transforma con **Pandas** y los carga en un **Data Warehouse** con modelo estrella listo para consumir desde **Power BI**.

---

## 📋 Tabla de contenidos

- [Arquitectura general](#arquitectura-general)
- [Tecnologías utilizadas](#tecnologías-utilizadas)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Servicios Docker](#servicios-docker)
- [El DAG: 3 tasks ETL](#el-dag-3-tasks-etl)
- [Modelo estrella (DWH)](#modelo-estrella-dwh)
- [Requisitos previos](#requisitos-previos)
- [Instalación y puesta en marcha](#instalación-y-puesta-en-marcha)
- [Credenciales y variables de entorno](#credenciales-y-variables-de-entorno)
- [Conexión con Power BI](#conexión-con-power-bi)
- [Comandos útiles](#comandos-útiles)

---

## Arquitectura general

```
BD Fuente (externa)
  PostgreSQL
  [credenciales en .env]
        │
        │  SQLAlchemy / psycopg2
        ▼
┌─────────────────────────────────────────┐
│           Docker Compose                │
│                                         │
│  ┌──────────────────────────────────┐   │
│  │        Apache Airflow 3.2.2      │   │
│  │                                  │   │
│  │  DAG: etl_ventas_pipeline        │   │
│  │  ┌──────────┐  ┌─────────────┐  │   │
│  │  │ extract  │→ │  transform  │  │   │
│  │  └──────────┘  └──────┬──────┘  │   │
│  │                        │         │   │
│  │                 ┌──────▼──────┐  │   │
│  │                 │    load     │  │   │
│  │                 └─────────────┘  │   │
│  └──────────────────────────────────┘   │
│                    │                    │
│                    ▼                    │
│         ┌──────────────────┐            │
│         │  postgres_dwh    │            │
│         │  Modelo estrella │            │
│         │  puerto: 5433    │            │
│         └──────────────────┘            │
└─────────────────────────────────────────┘
                    │
                    │  Conector PostgreSQL nativo
                    ▼
              📊 Power BI
```

> La **BD fuente no se levanta en Docker**. Solo se configuran sus credenciales como variables de entorno para que el DAG las consuma en tiempo de ejecución.

---

## Tecnologías utilizadas

| Tecnología | Versión | Rol |
|---|---|---|
| Apache Airflow | 3.2.2 | Orquestador del pipeline |
| Python | 3.12 | Lenguaje base del DAG |
| PostgreSQL | 15 | BD metadata de Airflow y DWH |
| Pandas | 2.2.2 | Transformación de datos |
| SQLAlchemy | 2.0.30 | Conexión a bases de datos |
| Docker / Docker Compose | Latest | Contenedorización |
| Power BI | Desktop | Visualización final |

---

## Estructura del proyecto

```
proyecto/
├── docker-compose.yml          # Orquesta todos los servicios
├── requirements.txt            # Dependencias Python del pipeline
├── README.md
│
├── dags/
│   └── etl_ventas_pipeline.py  # DAG con las 3 tasks ETL
│
├── init_dwh/
│   └── 01_create_star_schema.sql  # Crea el modelo estrella al iniciar el DWH
│
├── logs/                       # Logs de Airflow (generado automáticamente)
└── plugins/                    # Plugins personalizados de Airflow (opcional)
```

---

## Servicios Docker

El `docker-compose.yml` levanta **4 servicios**:

### `postgres_meta`
Base de datos interna que Airflow usa para almacenar sus propios metadatos: definición de DAGs, historial de ejecuciones, estado de tasks, logs, conexiones y variables. **No contiene datos del negocio.**

- Puerto: interno (no expuesto al host)
- Usuario: `airflow` / Contraseña: `airflow`
- Base de datos: `airflow`

### `postgres_dwh`
El **Data Warehouse** donde se almacena el resultado del pipeline en modelo estrella. Es la base de datos que consume Power BI.

- Puerto: `5433` (expuesto al host para conectar Power BI)
- Usuario: `dwh_user` / Contraseña: `dwh_password`
- Base de datos: `datawarehouse`

> Se expone en el puerto `5433` para evitar conflictos si tienes PostgreSQL instalado localmente en el `5432`.

### `airflow-webserver`
Interfaz web de Airflow para monitorear y gestionar los DAGs.

- URL: [http://localhost:8080](http://localhost:8080)
- Usuario: `admin` / Contraseña: `admin`

### `airflow-scheduler`
Proceso en segundo plano que evalúa los DAGs y lanza las ejecuciones según el schedule configurado. Corre en el mismo contenedor base que el webserver pero como servicio independiente.

---

## El DAG: 3 tasks ETL

El archivo `dags/etl_ventas_pipeline.py` define el pipeline completo. Se ejecuta **diariamente** (`@daily`) y está compuesto por 3 tasks encadenadas:

```
extract  →  transform  →  load
```

### Task 1 — `extract`
- Se conecta a la BD fuente usando las credenciales de las variables de entorno
- Lee las tablas necesarias con `pd.read_sql()`
- Serializa los DataFrames a JSON y los pasa al siguiente task via **XCom**

```python
clientes  = pd.read_sql("SELECT * FROM clientes",  conn)
productos = pd.read_sql("SELECT * FROM productos", conn)
ventas    = pd.read_sql("SELECT * FROM ventas",    conn)
```

### Task 2 — `transform`
- Recibe los datos crudos desde XCom y los reconstruye como DataFrames
- Aplica limpieza: elimina duplicados, maneja nulos, normaliza formatos
- Construye las **tablas de dimensiones** (`dim_cliente`, `dim_producto`, `dim_tiempo`)
- Calcula métricas derivadas: `total_neto = cantidad × precio × (1 - descuento)`
- Genera `dim_tiempo` automáticamente a partir de las fechas (año, mes, trimestre, día de semana, etc.)

### Task 3 — `load`
- Recibe las tablas transformadas desde XCom
- Inserta las dimensiones en el DWH con `ON CONFLICT DO UPDATE` (operación **idempotente** — se puede ejecutar varias veces sin duplicar datos)
- Resuelve las **claves foráneas** del DWH mapeando los IDs originales a los IDs del modelo estrella
- Inserta los registros en `fact_ventas` con las FK ya resueltas

---

## Modelo estrella (DWH)

El script `init_dwh/01_create_star_schema.sql` se ejecuta automáticamente cuando el contenedor `postgres_dwh` inicia por primera vez.

```
                dim_tiempo
                    │
dim_cliente ── fact_ventas ── dim_producto
```

| Tabla | Tipo | Descripción |
|---|---|---|
| `fact_ventas` | Hechos | Transacciones de venta con métricas (cantidad, precio, total) |
| `dim_cliente` | Dimensión | Datos del cliente (nombre, ciudad, segmento) |
| `dim_producto` | Dimensión | Datos del producto (nombre, categoría, precio unitario) |
| `dim_tiempo` | Dimensión | Desglose de fechas (año, mes, trimestre, día de semana) |

---

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) y [Docker Compose](https://docs.docker.com/compose/) instalados
- Mínimo **4 GB de RAM** asignados a Docker
- Puerto `8080` y `5433` libres en el host
- Credenciales de acceso a la BD fuente PostgreSQL externa

---

## Instalación y puesta en marcha

### 1. Clonar o descargar el proyecto

```bash
git clone <url-del-repositorio>
cd proyecto
```

### 2. Crear las carpetas necesarias

```bash
mkdir -p dags logs plugins init_dwh
```

### 3. Inicializar Airflow (primera vez)

Este paso crea las tablas internas de Airflow, instala las dependencias Python y crea el usuario `admin`:

```bash
docker compose up airflow-init
```

Espera a que aparezca el mensaje `airflow-init exited with code 0` antes de continuar.

### 4. Levantar todos los servicios

```bash
docker compose up -d
```

### 5. Verificar que todo esté corriendo

```bash
docker compose ps
```

Todos los servicios deben aparecer en estado `healthy` o `running`.

### 6. Acceder a la UI de Airflow

Abre [http://localhost:8080](http://localhost:8080) en tu navegador.

- Usuario: `admin`
- Contraseña: `admin`

Activa el DAG `etl_ventas_pipeline` y ejecútalo manualmente con el botón ▶️.

---

## Credenciales y variables de entorno

Las credenciales se configuran directamente en el `docker-compose.yml` bajo la sección `environment` del bloque `x-airflow-common`:

```yaml
# BD Fuente (externa)
SOURCE_DB_HOST: 'host_de_tu_bd_fuente'
SOURCE_DB_PORT: '5432'
SOURCE_DB_NAME: 'nombre_bd_fuente'
SOURCE_DB_USER: 'usuario_fuente'
SOURCE_DB_PASS: 'password_fuente'

# DWH (contenedor Docker)
DWH_DB_HOST: 'postgres_dwh'
DWH_DB_PORT: '5432'
DWH_DB_NAME: 'datawarehouse'
DWH_DB_USER: 'dwh_user'
DWH_DB_PASS: 'dwh_password'
```

> ⚠️ Para producción, mueve las credenciales a un archivo `.env` y agrégalo al `.gitignore`.

---

## Conexión con Power BI

1. Abre **Power BI Desktop**
2. Selecciona **Obtener datos → Base de datos PostgreSQL**
3. Ingresa los siguientes datos:

| Campo | Valor |
|---|---|
| Servidor | `localhost` |
| Puerto | `5433` |
| Base de datos | `datawarehouse` |
| Usuario | `dwh_user` |
| Contraseña | `dwh_password` |

4. Selecciona las tablas `fact_ventas`, `dim_cliente`, `dim_producto` y `dim_tiempo`
5. Las relaciones entre tablas ya están definidas por las FK — Power BI las detecta automáticamente

---

## Comandos útiles

```bash
# Ver logs de un servicio específico
docker compose logs -f airflow-scheduler

# Reiniciar un servicio
docker compose restart airflow-webserver

# Detener todo sin borrar datos
docker compose down

# Detener y borrar volúmenes (⚠️ borra el DWH)
docker compose down -v

# Ejecutar el DAG manualmente desde CLI
docker compose exec airflow-webserver airflow dags trigger etl_ventas_pipeline

# Ver el estado de los DAGs
docker compose exec airflow-webserver airflow dags list
```
