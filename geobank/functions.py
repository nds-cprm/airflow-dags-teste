import geopandas as gpd
import logging
import numpy as np
import oracledb
import pandas as pd

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
    logger.info("Os dados serão escritos no diretório %s", out_dir.absolute().as_posix())

    # Consulta a litoestratigrafia
    with open(BASE / "sql/main/litoestratigrafia_2.5M.sql", "r") as f:
        sql_query = f.read()

    logger.info("Buscando tabela principal...")
    geodata = (
        hook.get_pandas_df(sql_query)
            .rename(lambda val: val.lower(), axis="columns") # Forçar para minúsculas
            .set_index("fid")
    )
    logger.info("Tabela principal baixada, agora converter para GeoDataFrame")

    out_main = out_dir / "litoestratigrafia_2.5M.parquet"
    out_geometry = gpd.GeoSeries.from_wkb(
        geodata.pop("geometry"), 
        crs="EPSG:4326", 
        on_invalid="warn"
    )

    if not out_main.exists():
        (
            gpd.GeoDataFrame(geodata, geometry=out_geometry)
                .to_parquet(out_main, index=True, write_covering_bbox=True)
        )

    else:
        logger.warning("Arquivo já existe, continuando")

    logger.info("GeoDataFrame criado")

    out_files = {
        "main": out_main.as_posix(),
        "related": {}
    }

    # tabelas associadas a partir da pkey
    logger.info("Buscando tabelas associadas...")

    for item in ("litologias", "siglas_anteriores", "cores"):
        with open(BASE / f"sql/related/litoestratigrafia_{item}.sql", "r") as f:
            sql_query = f.read()

        out_related = out_dir / f"litoestratigrafia_{item}.parquet"

        logger.info("Buscando tabela %s...", item)
        (
            hook.get_pandas_df(sql_query)
                .rename(lambda val: val.lower(), axis="columns")
                .to_parquet(out_related)
        )
        logger.info("Tabela %s concluída", item)

        out_files["related"][item] = out_related.as_posix()

    return out_files


def sanitize_data(**kwargs):
    ti = kwargs["ti"]
    bronze_data = ti.xcom_pull("extract_from_oracle")

    base_df = (
        gpd.read_parquet(bronze_data["main"])
            .reset_index()
    )

    base_df = (
        base_df.merge(
            (
                pd.read_parquet(bronze_data["related"]["litologias"])
                    .groupby('id_unidade_estratigrafica')['nome_pt']
                    .agg(list)
                    .rename("litologias")
                    .to_frame()
            ),
            how="left",
            on="id_unidade_estratigrafica"
        )
        .merge(
            (
                pd.read_parquet(bronze_data["related"]["siglas_anteriores"])
                    .rename(lambda val: val.lower(), axis="columns")
                    .groupby('id_unidade_estratigrafica')['sigla_anterior']
                    .agg(list)
                    .rename("siglas_anteriores")
                    .to_frame()
            ),
            how="left",
            on="id_unidade_estratigrafica"
        )
        .drop("id_unidade_estratigrafica", axis="columns")
        .set_index("fid")
    )

    # Colocar na posição
    for column_to_move, col_to_put_after in [("litologias", "mapa"), ('siglas_anteriores', "epoca_max")]:
        base_df.insert(
            base_df.columns.tolist().index(col_to_put_after) + 1, 
            column_to_move, 
            base_df.pop(column_to_move)
        )

    base_df.info()

    out_silver_folder = get_silver_out_dir("litoestratigrafia_2.5M")
    out_file = out_silver_folder / "litoestratigrafia_2.5M_silver.parquet"

    base_df.to_parquet(out_file, index=True)

    return out_file.as_posix()
    


# # Consulta paginada
# def extract_paged_data(sql, page_size=1000):
#     # Inicializa o hook do Oracle configurado no Airflow
#     oracle_hook = _get_oracle_hook()
    

#     page_num = 1
#     ha_mais_dados = True

#     while ha_mais_dados:
#         # Define os limites das linhas para a página atual
#         start_row = ((page_num - 1) * page_size) + 1
#         end_row = page_num * page_size

#         # SQL de paginação obrigatório para Oracle 11g
#         sql = """
#             SELECT * FROM (
#                 SELECT a.*, ROWNUM rnum FROM (
#                     SELECT * FROM sua_tabela ORDER BY id_coluna
#                 ) a WHERE ROWNUM <= :end_row
#             ) WHERE rnum >= :start_row
#         """

#         # Executa a consulta passando os parâmetros de paginação
#         parameters = {'start_row': start_row, 'end_row': end_row}
#         rows = oracle_hook.get_records(sql=sql, parameters=parameters)

#         if rows:
#             # Processa o lote de dados retornado (lista de tuplas)
#             # for linha in registros:
#             #     print(linha)
#             yield rows
            
#             # Avança para a próxima página
#             page_num += 1
#         else:
#             # Encerra o loop quando não houver mais registros
#             ha_mais_dados = False
