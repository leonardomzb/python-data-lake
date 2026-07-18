# 📚 Pipeline Medallion: Buscalibre Scraper & Data Lake Lab

Proyecto personal de ingenieria de datos bajo la arquitectura **Medallion (Bronze -> Silver -> Gold)**. El flujo automatiza la extracción, estructuración, limpieza, , modelado dimensional y almacenamiento del ranking de los libros más vendidos en Chile desde la plataforma Buscalibre.


## 🛠️ Caracteristicas

*   **Orquestación:** **Airflow 3.3.0**.
*   **Entorno de Desarrollo:** **Docker Compose** con aislamiento de entorno virtual a través de **`uv`** y `pyproject.toml`.
*   **Ingesta (Capa Bronze):** Extracción mediante **`HTTPX`** y **`BeautifulSoup4`**.
*   **Procesamiento (Capa Silver):** Limpieza, tipado de datos y desestructuración de metadatos con **`Polars`**.
*   **Analítica (Capa Gold):** Modelado dimensional (Star Schema) separando hechos y dimensiones mediante claves hash con **`Polars`**.
*   **Formato de Almacenamiento:** Particionamiento físico en disco emulando el estándar **Hive** y archivos **`Parquet`**.


## Cómo replicar el Laboratorio en Local (Modo Pruebas)

Seguir estos pasos para levantar toda la infraestructura de Airflow y probar las capas Bronze, Silver y Gold:

### 1. Clonar el repositorio e instalar dependencias con uv
```bash
git clone <URL_DE_TU_REPOSITORIO>
cd <NOMBRE_DEL_DIRECTORIO>
uv sync
```

### 2. Configurar las Variables de Entorno mediante copia de archivo de ejemplo
```bash
# En Linux / macOS:
cp .env.example .env

# En Windows (CMD):
copy .env.example .env
```

### 3. Levantar la infraestructura con Docker
```bash
docker compose up -d
```

*(Nota: Ultima actualización en julio de 2026. Si el portal BuscaLibre modificó su diseño, es posible que el pipeline falle).*