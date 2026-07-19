import os
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pyarrow as pa
from bs4 import BeautifulSoup
from dotenv import load_dotenv


def ingesta_bronze(url: str, headers: dict[str, str], fecha_logica: str, raiz_proyecto: Path) -> str | None:
    """
    Extracción desde Buscalibre y almacenamiento
    de los datos en formato Parquet dentro de la capa Bronze.
    """

    with httpx.Client(headers=headers, http2=True, timeout=12.0) as client:
        try:
            print(f"Descargando datos desde: {url}")
            response = client.get(url)

            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            bloques_libros = soup.find_all("div", class_="box-producto")

            if not bloques_libros:
                raise ValueError("Error crítico: No se encontraron bloques de libros en el HTML.")

            current_time = datetime.strptime(fecha_logica, "%Y%m%d_%H%M%S")

            records: list[dict[str, Any]] = []

            for posicion, bloque in enumerate(bloques_libros, start=1):
                # Campos obligarotorios, de haber modificacion de diseño en BuscaLibre,
                # se espera un error para actualizacion del script.
                id_producto = bloque["data-id_producto"]
                titulo = bloque.find("h3", class_="nombre").text.strip()
                autor = bloque.find("div", class_="autor").text.strip()
                precio_final = bloque["data-precio"]
                metadatos = bloque.find("div", class_="metas").text.strip()

                # Campos flexibles, se pueden almacenar como "None"
                tag_desc = bloque.find("div", class_="descuento-v2")
                porcentaje_descuento = tag_desc.text.strip() if tag_desc else None

                tag_del = bloque.find("del")
                precio_antes = tag_del.text.strip() if tag_del else None

                records.append(
                    {
                        "ranking": posicion,
                        "id_producto": id_producto,
                        "titulo": titulo,
                        "autor": autor,
                        "metadatos": metadatos,
                        "porcentaje_descuento": porcentaje_descuento,
                        "precio_final": precio_final,
                        "precio_antes": precio_antes,
                        "fecha_ingesta": current_time.strftime("%Y-%m-%d"),
                    }
                )

            # Creacion de directorios con estándar Hive.
            lake_dir = raiz_proyecto / "lake" / "bronze" / "libros_mas_vendidos"
            bronce_folder: Path = (
                lake_dir / f"year={current_time:%Y}" / f"month={current_time:%m}" / f"day={current_time:%d}"
            )
            bronce_folder.mkdir(parents=True, exist_ok=True)

            bronce_file_path: Path = bronce_folder / f"data_{fecha_logica}.parquet"

            # records a tabla arrow
            tabla_arrow = pa.Table.from_pylist(records)

            print("Guardando datos estructurados en la Capa Bronce...")
            # Conexión explícita y aislada en memoria para DuckDB
            with duckdb.connect(database=":memory:") as con:
                con.from_arrow(tabla_arrow).write_parquet(
                    bronce_file_path.as_posix(),
                    compression="ZSTD"
                )

            print(f"Éxito. Archivo estructurado guardado en Bronce: {bronce_file_path}")
            return bronce_file_path.as_posix()

        except httpx.HTTPStatusError as http_err:
            print(f"Error de red o código de estado HTTP: {http_err}")
            raise
        except KeyError as key_err:
            print(f"Error de estructura HTML: {key_err}")
            raise
        except AttributeError as attr_err:
            print(f"Error de estructura HTML: {attr_err}")
            raise
        except Exception as e:
            print(f"Error crítico inesperado en la capa Bronce: {e}")
            raise


if __name__ == "__main__":
    # Carga de variables de entorno
    load_dotenv()
    URL_LOCAL = os.getenv("BUSCALIBRE_URL")
    USER_AGENT_LOCAL = os.getenv("USER_AGENT")

    # Simulación de cabeceras de navegador (User-Agent) para evitar bloqueos
    HEADERS_LOCAL = {"User-Agent": USER_AGENT_LOCAL}

    # Valores default solo para pruebas de script individuales
    DEFAULT_ROOT: Path = Path(__file__).resolve().parents[2]
    FECHA_DEFAULT: str = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("Prueba local bronze...")
    ingesta_bronze(url=URL_LOCAL, headers=HEADERS_LOCAL, fecha_logica=FECHA_DEFAULT, raiz_proyecto=DEFAULT_ROOT)
