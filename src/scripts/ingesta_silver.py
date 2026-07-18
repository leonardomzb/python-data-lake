from pathlib import Path
from datetime import datetime
import polars as pl


def procesar_silver(bronce_file: str, fecha_logica: str,raiz_proyecto: Path) -> bool:
  try:
      current_time = datetime.strptime(fecha_logica, "%Y%m%d_%H%M%S")

      query = pl.scan_parquet(bronce_file)
      print("Comenzando a procesar capa Silver...")
      transformacion_capa_silver = (
        query.select([      
          
          "ranking",
          "id_producto",
          "titulo",
          "autor",

          # Extracción de editorial de la columna metadatos
          pl.col("metadatos")
          .str.extract(r"^([^,]+)")
          .str.strip_chars()
          .alias("editorial"),

          # Extracción del año si existe de la columna metadatos
          pl.col("metadatos")
            .str.extract(r"(\b\d{4}\b)")
            .cast(pl.Int32, strict=False)
            .fill_null(0)
            .alias("anio"),

          # Extracción de edicion si existe de la columna metadatos
          pl.col("metadatos")
            .str.extract(r"([^,]*\bedici[óo]n\b[^,]*)")
            .str.strip_chars()
            .fill_null("n/a")
            .alias("edicion"),

          # Extracción del estado del libro de la columna metadatos
          pl.col("metadatos")
            .str.extract(r"\b(Nuevo|Usado|Como nuevo)\b")
            .str.strip_chars()
            .alias("estado"),

          # Extracción de tipo de libro de la columna metadatos
          pl.col("metadatos")
            .str.extract(r"\b(Tapa Blanda|Tapa Dura|Rústica con Solapas|Rústica)\b")
            .str.strip_chars()
            .fill_null("n/a")
            .alias("tipo_libro"),

          # Se usó replace_all para dejar solo numeros en caso que se agreguen otros caracteres.
          pl.col("precio_final").str.replace_all(r"\D", "").cast(pl.Int32), 
          
          # Se extraen solo los 2 primeros numeros
          pl.col("porcentaje_descuento").str.extract(r"^(\d+)").cast(pl.Int32 , strict=False).fill_null(0),        
          
          # Se extraen solo los numeros
          pl.col("precio_antes").str.replace_all(r"\D", "").cast(pl.Int32, strict=False).fill_null(0),

          pl.col("fecha_ingesta").str.to_datetime().dt.date()    

        ])
      )
      
      # Creacion de directorios de capa silver con estándar Hive.
      lake_dir = raiz_proyecto / "lake" / "silver" / "libros_mas_vendidos"
      silver_folder: Path = lake_dir / f"year={current_time:%Y}" / f"month={current_time:%m}" / f"day={current_time:%d}"
      silver_folder.mkdir(parents=True, exist_ok=True)

      print("Guardando datos estructurados en la capa Silver...")
      silver_file_path: Path = silver_folder / f"data_{fecha_logica}.parquet"

      transformacion_capa_silver.sink_parquet(silver_file_path, compression="zstd")
      
      print(f"Éxito. Archivo estructurado guardado en Silver: {silver_file_path}")
      return silver_file_path.as_posix()
  
  except Exception as e:
            print(f"Error crítico inesperado en la capa Silver: {e}")
            raise

if __name__ == "__main__":
    #Valores default solo para pruebas de script individuales
    BRONCE_FILE: str = r'C:\Users\leona\Documents\Trabajo\Trabajo\Python\python-data-lake\lake\bronze\libros_mas_vendidos\year=2026\month=07\day=18\data_20260718_134816.parquet'

    DEFAULT_ROOT: Path = Path(__file__).resolve().parents[2]
    FECHA_DEFAULT: str = datetime.now().strftime("%Y%m%d_%H%M%S") 

    print("Prueba local silver...")
    procesar_silver(         
         bronce_file= BRONCE_FILE,
         fecha_logica=FECHA_DEFAULT,
         raiz_proyecto=DEFAULT_ROOT
    )