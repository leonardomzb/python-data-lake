from datetime import datetime
from pathlib import Path

import polars as pl


def generar_gold(silver_file: str, fecha_logica: str, raiz_proyecto: Path) -> bool:
    try:
        current_time = datetime.strptime(fecha_logica, "%Y%m%d_%H%M%S")
        print("Comenzando a procesar datos...")
        df_silver = pl.scan_parquet(silver_file)

        print("Generando dim_editorial...")
        dim_editorial = (
            df_silver.select(["editorial"])
            .with_columns([pl.col("editorial").str.to_lowercase().alias("texto_limpio")])
            .unique(subset=["texto_limpio"])
            .with_columns(
                [pl.col("texto_limpio").hash().alias("id_editorial"), pl.col("editorial").alias("nombre_editorial")]
            )
            .select(["id_editorial", "nombre_editorial"])
        )
        print("Generando dim_autor...")
        dim_autor = (
            df_silver.select(["autor"])
            .with_columns([pl.col("autor").str.to_lowercase().alias("texto_limpio")])
            .unique(subset=["texto_limpio"])
            .with_columns([pl.col("texto_limpio").hash().alias("id_autor"), pl.col("autor").alias("nombre_autor")])
            .select(["id_autor", "nombre_autor"])
        )
        print("Generando dim_libro...")
        dim_libro = (
            df_silver.select(["id_producto", "titulo", "autor", "editorial"])
            .unique(subset=["id_producto"])
            .with_columns(
                [
                    pl.col("id_producto").alias("id_libro"),
                    pl.col("titulo").str.strip_chars().alias("titulo_libro"),
                    pl.col("autor").str.strip_chars().str.to_lowercase(),
                    pl.col("editorial").str.strip_chars().str.to_lowercase(),
                ]
            )
            .join(
                dim_autor.with_columns(pl.col("nombre_autor").str.to_lowercase()),
                left_on="autor",
                right_on="nombre_autor",
                how="left",
            )
            .join(
                dim_editorial.with_columns(pl.col("nombre_editorial").str.to_lowercase()),
                left_on="editorial",
                right_on="nombre_editorial",
                how="left",
            )
            .select(["id_libro", "titulo_libro", "id_autor", "id_editorial"])
        )
        print("Generando fact_ranking...")
        fact_ranking = (
            df_silver.select(["ranking", "id_producto", "fecha_ingesta"])
            .unique(subset=["id_producto", "fecha_ingesta"])
            .with_columns(
                [
                    pl.col("id_producto").alias("id_libro"),
                    pl.col("ranking"),
                    pl.col("fecha_ingesta").alias("fecha_ranking"),
                ]
            )
            .select(["id_libro", "ranking", "fecha_ranking"])
        )

        lake_dir = raiz_proyecto / "lake" / "gold" / "libros_mas_vendidos"
        gold_folder: Path = lake_dir / f"year={current_time:%Y}" / f"month={current_time:%m}" / f"day={current_time:%d}"
        gold_folder.mkdir(parents=True, exist_ok=True)

        dim_editorial_file_path: Path = gold_folder / f"dim_editorial_{fecha_logica}.parquet"
        dim_autor_file_path: Path = gold_folder / f"dim_autor_{fecha_logica}.parquet"
        dim_libro_file_path: Path = gold_folder / f"dim_libro_{fecha_logica}.parquet"
        fact_ranking_file_path: Path = gold_folder / f"fact_ranking_{fecha_logica}.parquet"

        dim_editorial.sink_parquet(dim_editorial_file_path)
        dim_autor.sink_parquet(dim_autor_file_path)
        dim_libro.sink_parquet(dim_libro_file_path)
        fact_ranking.sink_parquet(fact_ranking_file_path)

        print("¡Proceso de ingesta Gold finalizado con éxito!")
        rutas_gold = {
            "dim_editorial": str(dim_editorial_file_path),
            "dim_autor": str(dim_autor_file_path),
            "dim_libro": str(dim_libro_file_path),
            "fact_ranking": str(fact_ranking_file_path),
        }
        return rutas_gold

    except Exception as e:
        print(f"Error crítico inesperado en la capa Gold: {e}")
        raise


if __name__ == "__main__":
    # Valores default solo para pruebas de script individuales
    SILVER_FILE: str = (
        r"C:\Users\leona\Documents\Trabajo\Trabajo"
        r"\Python\python-data-lake\lake\silver\libros_mas_vendidos"
        r"\year=2026\month=07\day=18\data_20260718_134853.parquet"
    )
    FECHA_DEFAULT: str = datetime.now().strftime("%Y%m%d_%H%M%S")
    DEFAULT_ROOT: Path = Path(__file__).resolve().parents[2]

    print("Prueba local gold...")
    generar_gold(silver_file=SILVER_FILE, fecha_logica=FECHA_DEFAULT, raiz_proyecto=DEFAULT_ROOT)
