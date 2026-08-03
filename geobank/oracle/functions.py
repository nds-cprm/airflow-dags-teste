import geopandas as gpd
import logging
import oracledb

from airflow.models import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from pathlib import Path

from sgb.common.medallion import get_bronze_out_dir, get_silver_out_dir


logger = logging.getLogger("airflow.task")

BASE = Path(__file__).parent

# Função auxiliar para instanciar o cliente OpenSearch otimizado
def _get_oracle_hook():
    conn_id = Variable.get("GEOSGB_GEOBANK_CONNECTION", "oracle_default")
    hook = OracleHook(oracle_conn_id=conn_id)

    return hook


def extract_data_from_oracle(**kwargs):
    oracledb.defaults.fetch_lobs = False

    hook = _get_oracle_hook()
    out_dir = get_bronze_out_dir("litoestratigrafia_2.5m")

    # Consulta a litoestratigrafia
    with open(BASE / "sql/litoestratigrafia_2.5M.sql", "r") as f:
        sql_query = f.read()

    logger.info("Buscando tabela principal...")
    geodata = hook.get_pandas_df(sql_query) # Forçar para minúsculas
    logger.info(geodata.columns)
    logger.info("Tabela principal baixada, agora converter para GeoDataFrame")

    gpd.GeoDataFrame(
        geodata,
        index=geodata.index,
        geometry=gpd.GeoSeries.from_wkb(geodata["GEOMETRY"]),
        crs="EPSG:4326"
    ).to_parquet(out_dir / "litoestratigrafia_1M.parquet", index=True, write_covering_bbox=True)

    logger.info("geoDataFrame criado")

    # tabelas associadas a partir da pkey
    logger.info("Buscando tabelas associadas...")

    for item in ("litoestratigrafia_litologias", "litoestratigrafia_simbolos_anteriores", "litoestratigrafia_cores"):
        with open(BASE / f"sql/{item}", "r") as f:
            sql_query = f.read()

        logger.info("Buscando tabela %s...", item)
        hook.get_pandas_df(sql_query).to_parquet(out_dir / f"{item}.parquet")
        logger.info("Tabela %s concluída", item)
    