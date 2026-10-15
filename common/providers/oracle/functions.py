# import geopandas as gpd
import logging
import oracledb

from airflow.models import Variable
from pathlib import Path

from sgb.common.providers.oracle.hooks import GeoPandasOracleHook
from sgb.common.medallion import get_bronze_out_dir #, get_silver_out_dir 


logger = logging.getLogger("airflow.task")


def _get_oracle_hook(**kwargs) -> GeoPandasOracleHook:
    """
    """
    conn_id = Variable.get("GEOSGB_GEOBANK_CONNECTION", "oracle_default")
    hook = GeoPandasOracleHook(oracle_conn_id=conn_id)

    # Oracle defaults
    # https://python-oracledb.readthedocs.io/en/latest/api_manual/defaults.html
    for k, v in kwargs.items():
        if getattr(oracledb.defaults, k):
            logger.info("Definindo comportamentos para a conexão do Oracle: %s=%s", k, v)
            setattr(oracledb.defaults, k, v)

    return hook


def get_bronze_database(dataset_name, **kwargs) -> str:
    return (get_bronze_out_dir(dataset_name) / f"{dataset_name}.gpkg").as_posix()


def extract_sql_query_to_gpkg(
    gpkg,
    layer_name, 
    sql, 
    parameters=None,
    chunksize=10000,
    normalize=True, 
    overwrite=True,
    geometry_col=None, 
    crs="EPSG:4326", 
    **kwargs
):
    """
    """
    hook = _get_oracle_hook(fetch_lobs=False)
    out_file = Path(gpkg)

    # Inicia a gravação do geopackage
    if out_file.exists() and not overwrite:
        logger.warning("O dataset solicitado já existe: %s", out_file)

    else:
        rows_written = 0

        chunk_kwargs = {
            "sql": sql, 
            "chunksize": chunksize,
            "geometry_col": geometry_col, 
            "crs": crs,
        }

        if overwrite:
            logger.warning("Sobrescrevendo o dataset: %s", out_file)

        if parameters:
            chunk_kwargs["parameters"] = parameters

        for i, chunk_df in enumerate(hook.get_geodf_from_geometry_by_chunks(**chunk_kwargs)):
            # normalizar para lower case
            if normalize:
                chunk_df.rename(lambda col: col.lower(), axis="columns", inplace=True)

            # Gravar gpkg
            write_gpkg_args = {
                "filename": out_file, 
                "layer": layer_name, 
                "driver": "GPKG", 
                "engine": "pyogrio",
                # "index": True,
            }

            if i == 0:
                # Primeiro chunk: cria o arquivo do zero (modo 'w')
                chunk_df.to_file(mode="w", **write_gpkg_args)                
            else:
                # Chunks seguintes: anexa ao arquivo existente (modo 'a')
                chunk_df.to_file(mode="a", **write_gpkg_args)

            rows_written += chunk_df.shape[0]
        
        logger.info("GeoDataFrame criado! Foram salvos %s registros", rows_written)

    # return out_file.as_posix()


# def sanitize_data_and_export_to_geojson(in_gpkg, feature_class, **kwargs) -> str:
#     """
#     """
#     data = gpd.read_file(in_gpkg, engine="pyogrio", layer="")