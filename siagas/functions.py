import geopandas as gpd
import logging
import hashlib

from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from pathlib import Path

from sgb.common.medallion import get_bronze_out_dir, get_silver_out_dir


logger = logging.getLogger("airflow.task")

BASE = Path(__file__).parent


# Função auxiliar para instanciar hook do SQLServer
def _get_mssql_hook():
    conn_id = Variable.get("GEOSGB_SIAGAS_CONNECTION", "siagas_default")
    hook = MsSqlHook(mssql_conn_id=conn_id)

    return hook

# função auxiliar para calcular hash de arquivo
def calc_hash(in_file, algorithm="md5", chunksize=8192):
    Hash = getattr(hashlib, algorithm)
    _hash = Hash()

    with open(in_file, "rb") as f:
        for chunk in iter(lambda: f.read(chunksize), b""):
            _hash.update(chunk)
    
    return f"{algorithm}:{_hash.hexdigest()}"


def extract_data_from_siagas(**kwargs):
    hook = _get_mssql_hook()
    dataset_name = "siagas"

    out_dir = get_bronze_out_dir(dataset_name)
    logger.info("Os dados serão escritos no diretório %s", out_dir.absolute().as_posix())

    # Consulta ap SIAGAS
    with open(BASE / "sql/siagas.sql", "r") as f:
        sql_query = f.read()

    logger.info("Buscando tabela principal...")

    category_columns = [
        "geocodigo_municipio", "nome_municipio", "uf", "sureg", "natureza_ponto", 
        "tipo_situacao", "uso_agua", "aquifero", "status_rimas"
    ]
    
    geodata = (
        hook.get_pandas_df(sql_query, dtype={col: "category" for col in category_columns})
            .rename(lambda val: val.lower(), axis="columns") # Forçar para minúsculas
            .set_index("idt_ponto")                          # TODO: VER QUESTão do nome do FID
    )
    logger.info("Tabela principal baixada, agora converter para GeoDataFrame")

    out_main = out_dir / f"{dataset_name}.parquet"
    out_geometry = gpd.GeoSeries.from_xy(
        x=geodata.pop("longitude"), 
        y=geodata.pop("latitude"), 
        crs="EPSG:4326", 
    )

    if not out_main.exists():
        geodata = gpd.GeoDataFrame(geodata, geometry=out_geometry)
        geodata.to_parquet(out_main, index=True, write_covering_bbox=True)

        logger.info("Exportado para o formato GeoParquet!")

    else:
        logger.warning("Arquivo já existe, continuando")

    logger.info(geodata.info())

    out_files = {
        "main": {
            "name": out_main.as_posix(),
            "hash": calc_hash(out_main)
        },
        # "related": {}
    }

    return out_files
