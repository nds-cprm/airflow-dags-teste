import logging

from airflow.datasets import Dataset, DatasetAlias
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException #, AirflowSkipException
from airflow.models import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.smtp.operators.smtp import EmailOperator
from pathlib import Path

from sgb.common import default_args


log = logging.getLogger("airflow.task")



# TODO: Trocar por JSON ou YAML carregado de arquivo
geobank_tables = {
    "aflora": {
        "dag": {
            # "name": "geobank_aflora",
            # "schedule_interval": None,
            "args": {
                "description": "AFLOra",
                "tags": ['geobank', 'ESRI', 'geologia', 'oracle'],
            },
        },
        "src": { 
            "connection": None,  # TODO: Permitir a sobrescita de connections
            "main": {"schema": "aflora", "table": "af_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        },
        "dst": { 
            "connection": None,
            "main": {"schema": "geobank", "table": "aflora_af_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        }
    },
    "petro": {
        "dag": {
            "name": None,
            "args": {
                "tags": ['geobank', 'ESRI', 'geologia', 'oracle'],
            },
        },
        "src": { 
            "connection": None,  # TODO: Permitir a sobrescita de connections
            "main": {"schema": "petrografia", "table": "pet_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        },
        "dst": { 
            "connection": None,
            "main": {"schema": "geobank", "table": "petrografia_pet_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        }
    },
    "recmin": {
        "dag": {
            "name": None,
            "args": {
                "tags": ['geobank', 'ESRI', 'recursos minerais', 'oracle'],
            },
        },
        "src": { 
            "connection": None,  # TODO: Permitir a sobrescita de connections
            "main": {"schema": "recmin", "table": "rm_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        },
        "dst": { 
            "connection": None,
            "main": {"schema": "geobank", "table": "recmin_rm_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        }
    },
    "paleo": {
        "dag": {
            "name": None,
            "args": {
                "tags": ['geobank', 'ESRI', 'geologia', 'oracle'],
            },
        },
        "src": { 
            "connection": None,  # TODO: Permitir a sobrescita de connections
            "main": {"schema": "paleo", "table": "pl_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        },
        "dst": { 
            "connection": None,
            "main": {"schema": "geobank", "table": "paleo_pl_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        }
    },
    "geocron": {
        "dag": {
            "name": None,
            "args": {
                "tags": ['geobank', 'ESRI', 'geologia', 'oracle'],
            },
        },
        "src": { 
            "connection": None,  # TODO: Permitir a sobrescita de connections
            "main": {"schema": "geocronologia", "table": "gr_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        },
        "dst": { 
            "connection": None,
            "main": {"schema": "geobank", "table": "geocronologia_gr_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        }
    },
    "lito": {
        "dag": {
            "name": None,
            "args": {
                "tags": ['geobank', 'ESRI', 'geologia', 'oracle'],
            },
        },
        "src": { 
            "connection": None,  # TODO: Permitir a sobrescita de connections
            "main": {"schema": "litoestratigrafia", "table": "ue_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        },
        "dst": { 
            "connection": None,
            "main": {"schema": "geobank", "table": "litoestratigrafia_ue_layer"},
            "related": [
                # {"schema": "aflora", "table": "af_layer"}
            ]
        }
    }
}


for config_name, config in geobank_tables.items():
    # dag name and args
    dag_id = config_name
    dag_args = {}
    schedule_interval = None

    if "dag" in config:
        dag_id = config["dag"].get('name', config_name) or config_name

        dag_args = config["dag"].get("args", dag_args) or dag_args
        assert isinstance(dag_args, dict)

        schedule_interval = config["dag"].get("schedule_interval", schedule_interval) or schedule_interval

    @dag(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        default_args=default_args,
        **dag_args
    )
    def geobank_etl():
        """
        _summary_
        """
        null_byte_str = "\x00"
        alias = DatasetAlias("geobank")

        @task
        def extract_sde_feature_classes(schema, table, **kwargs):
            """
            _summary_

            Args:
                connection (_type_): _description_
                schema (_type_): _description_
                table (_type_): _description_

            Returns:
                path with a parquet file with checksum (?)
            """
            import geopandas as gpd
            import oracledb
            import sys

            from sqlalchemy import MetaData, create_engine

            from utils.sde import STGeometry, SDETable

            sys.modules["cx_Oracle"] = oracledb
            oracledb.version = "8.3.0"

            oracledb.init_oracle_client()

            conn_name = Variable.get("GEOSGB_GEOBANK_CONNECTION", default_var="geobank_prod")
            # SQLAlchemy 1.4 does not support new oracle db
            # https://stackoverflow.com/a/74105559
            uri = OracleHook(oracle_conn_id=conn_name).get_uri().replace("+oracledb", "")
            src_engine = create_engine(uri)

            # Puxar dados do Oracle, já convertidos para WKB
            try:
                log.info("Fazendo a reflexão da tabela no Oracle <%s.%s>...", schema, table)
                src_metadata = MetaData(schema=schema)
                src_table = SDETable(table, src_metadata, autoload_with=src_engine)
                log.info("Tabela refletida com sucesso")

            except Exception as e:
                logging.error("Não foi possível refletir a tabela <%s.%s>: %s", schema, table, str(e))
                raise e
            
            # Metadados da tabela
            src_pk_col = src_table.sde.rowid_name
            src_geom_col = src_table.geometry_columns[0] # Vai pegar a primeira

            # Oracle para geopandas
            dst_pk_name = Variable.get("GEOSGB_FEATURE_ID_FIELD_NAME", default_var="fid")
            dst_geom_name = Variable.get("GEOSGB_GEOMETRY_FIELD_NAME", default_var="geometry")
            chunksize = Variable.get("GEOSGB_READ_CHUNKSIZE", default_var="20000")

            with src_engine.connect() as conn:
                logging.info("Carregando dados do Oracle em GeoDataFrame...")
                geodata = (
                    gpd.read_postgis(
                        str(src_table.select()), 
                        conn,
                        geom_col=src_geom_col.name,
                        crs=src_geom_col.type.srid,
                        # chunksize=int(chunksize)
                    )
                    .rename_geometry(dst_geom_name)
                    .rename(columns={src_pk_col: dst_pk_name})
                    .set_index(dst_pk_name)
                )

                if "MULTI" in src_geom_col.type.geometry_type.upper():
                    logging.info("Detectado MULTI: Forçar a conversão de SingleParts para MultiParts usando a função collect do GeoPandas")
                    
                    geodata[dst_geom_name] = geodata[dst_geom_name].apply(
                        lambda geom: gpd.tools.collect([geom], multi=True)
                    )

                logging.info("Dados carregados em GeoDataFrame")

            # Gravar em parquet 
            module_dir = Path("/tmp")
            out_parquet = module_dir / f"data/geobank_{schema}_{table}.parquet"

            try:
                logging.info("Exportando para GeoParquet...")
                out_parquet.parent.mkdir(parents=True, exist_ok=True)
                geodata.to_parquet(out_parquet , index=True)
                logging.info("Exportação concluída")

            except Exception as e:
                logging.warning("Problema na exportação do GeoParquet: %s", str(e))
                raise e

            return out_parquet.as_posix()
            

        @task(multiple_outputs=True)
        def analyze_dataset(dataset, **kwargs):
            """_summary_

            Args:
                dataset (_type_): _description_

            Returns:
                _type_: _description_
            """
            import geopandas as gpd
            from pyproj.crs import CRS

            geodata = gpd.read_parquet(dataset)
            geom_col = geodata.active_geometry_name
            
            # Checar chaves primárias duplicadas
            fid_is_duplicated = not geodata.index.is_unique
            logging.info("Checando duplicidade de FIDs: %s", fid_is_duplicated)
            assert not fid_is_duplicated, "O dataset contém FIDs duplicados. Abortar tarefa!"
            
            # Checar tipo de geometria
            logging.info("Tipo de geometria identificada: %s", ",".join(set(geodata[geom_col].geom_type.tolist())))
            
            # checar projeção do dado de origem
            src_crs = geodata[geom_col].crs
            dst_crs = CRS.from_epsg(int(Variable.get("GEOSGB_DEFAULT_SRID", default_var="4674")))
            crs_is_different = src_crs != dst_crs
            
            logging.info("SRID de origem: %s", src_crs.to_epsg())

            if crs_is_different:
                logging.warning(
                    "o SRID de destino <%s> é diferente do SRID de origem <%s>. O dataset será reprojetado para o SRID de destino", 
                    dst_crs.to_epsg(), 
                    src_crs.to_epsg()
                )

            # checar estruturas de geometria
            has_empty_geometry = bool(geodata[geom_col].is_empty.any())
            logging.info("Há ocorrência de geometrias vazias? %s", has_empty_geometry)

            has_invalid_geometry = not geodata[geom_col].is_valid.all()
            logging.info("Há ocorrência de geometrias inválidas? %s", has_invalid_geometry)

            # Verificar ocorrência de Nullbytes em atrbutos
            for col in geodata.select_dtypes(include="object").columns:
                has_null_byte_str = bool(geodata[col].str.contains(null_byte_str, regex=False).any())

                if has_null_byte_str:
                    logging.warning("A coluna <%s> contém Null Bytes <\x00> na cadeia de caracteres.", col)
            
            return {
                "dataset": dataset,
                "analysis_results": {
                    "has_null_byte_str": has_null_byte_str, 
                    "crs_is_different": crs_is_different, 
                    "has_empty_geometry": has_empty_geometry,
                    "has_invalid_geometry": has_invalid_geometry
                }
            }


        @task
        def clean_dataset(dataset, analysis_results, auto_repair_geometry=True, auto_repair_method="linework", **kwargs):
            import geopandas as gpd

            # I/O
            in_parquet = Path(dataset)
            out_gpkg = in_parquet.with_suffix(".gpkg")

            # analysis results
            has_null_byte_str = analysis_results.get("has_null_byte_str", False)
            crs_is_different = analysis_results.get("crs_is_different", False)
            has_empty_geometry = analysis_results.get("has_empty_geometry", False)
            has_invalid_geometry = analysis_results.get("has_invalid_geometry", False)

            # Load data
            geodata = gpd.read_parquet(in_parquet)

            # Tratamento de Null Bytes
            if has_null_byte_str: 
                logging.warning("Removendo null bytes <%s> das colunas de texto...", null_byte_str)
                str_cols = geodata.select_dtypes(include="object").columns
                geodata[str_cols] = geodata[str_cols].apply(lambda x: x.str.replace(null_byte_str, "", regex=False))
            
            # Tratamento de CRS
            if crs_is_different:            
                epsg = int(Variable.get("GEOSGB_DEFAULT_SRID", default_var="4674"))
                logging.warning("Reprojetando o dataset para o CRS %s.", epsg) 
                geodata.to_crs(epsg=epsg, inplace=True)
            
            # Geometrias vazias
            if has_empty_geometry:
                logging.warning("O dataset possui geometrias vazias: O banco de dados de destino não pode possuir restrição de NOT NULL")

            # Geometrias inválidas (GEOS/Shapely)
            if has_invalid_geometry:
                geom_col = geodata.active_geometry_name                        
                geometry_invalid_reason = geodata[~geodata[geom_col].is_valid][geom_col].is_valid_reason()

                logging.warning("O dataset possui %s registros com geometrias inválidas.", geometry_invalid_reason.size)
                geometry_invalid_reason.to_file(out_gpkg, driver='GPKG', index=True, layer="invalid")

                if auto_repair_geometry:
                    # https://shapely.readthedocs.io/en/latest/reference/shapely.make_valid.html
                    logging.info("A auto-correção será realizada pelo métdodo '%s'", auto_repair_method)
                    
                    geodata[geom_col] = geodata[geom_col].make_valid(method=auto_repair_method)
                    repaired_geometry_invalid_reason = geodata[~geodata[geom_col].is_valid][geom_col].is_valid_reason()

                    logging.info("Após a auto-correção, o dataset possui %s registros com geometrias inválidas", repaired_geometry_invalid_reason.size)
                    repaired_geometry_invalid_reason.to_file(out_gpkg, driver='GPKG', index=True, layer="invalid_after_autorepair")

                else:
                    logging.warning(
                        "A auto-correção está desativada",
                        auto_repair_geometry
                    )            
            
            logging.info("Salvando dados reparados em GeoPackage...")
            geodata.to_file(out_gpkg, driver='GPKG', index=True, layer="valid")
            
            return out_gpkg.as_posix()
        

        @task(outlets=[alias])
        def write_postgres(dataset, table, schema="public", **kwargs):
            # write tables, vaccum and reindex
            import geopandas as gpd
            from datetime import datetime
            from sqlalchemy import text

            # I/O 
            geodata = gpd.read_file(dataset, driver="GPKG", layer="valid")

            # postgres
            conn_name = Variable.get("GEOSGB_GOLD_CONNECTION", default_var="geosgb_gold")
            hook = PostgresHook(postgres_conn_id=conn_name)
            conn = hook.get_connection(conn_name)

            # Dataset event
            outlet_events = kwargs["outlet_events"]
            outlet_events[alias].add(
                Dataset(f"postgres://{conn.host}:{conn.port}/{conn.schema}/{schema}/{table}"),
                extra={
                    "rows_inserted": geodata.index.size,
                    "updated_at": datetime.now().isoformat()
                }
            )

            # write postgres
            with hook.get_sqlalchemy_engine().connect() as c:
                c.execute(text(f"TRUNCATE TABLE {schema}.{table};"))

                geodata.to_postgis(
                    table, 
                    c, 
                    schema=schema, 
                    if_exists="append", 
                    index=True, 
                    index_label="fid",
                    chunksize=5000
                )

                c.execute(text(f"ANALYZE {schema}.{table};"))

        # call tasks - Add values from dict here
        # TODO: Check if this key exists and raise exception on dag analysis
        src_schema, src_table = config["src"]["main"]["schema"], config["src"]["main"]["table"]
        dst_schema, dst_table = config["dst"]["main"]["schema"], config["dst"]["main"]["table"]

        dataset = extract_sde_feature_classes(src_schema, src_table)
        results = analyze_dataset(dataset)
        cleaned_dataset = clean_dataset(results['dataset'], results['analysis_results'])    
        write_postgres(cleaned_dataset, schema=dst_schema, table=dst_table)

    # call dag
    geobank_etl()


# if __name__ == '__main__':
#     #_dag.test()
#     pass
