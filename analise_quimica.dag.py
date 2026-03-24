import logging


# from airflow.datasets import Dataset, DatasetAlias
from airflow.decorators import dag, task
# from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.smtp.operators.smtp import EmailOperator
from slugify import slugify
from sqlalchemy import text

from teste.common import default_args, export_parquet
from teste.common.postgres import get_postgres_table_colums


log = logging.getLogger("airflow.task")

# TODO: Colocar dag_args neste objeto
classes = {
    'rocha': {
        "schedule": None,
    },
    'sedimento_de_corrente': 
    {
        
    },
}


for classe, dag_config in classes.items():
    @dag(
        dag_id=f"analise_quimica_{classe}",
        schedule=None,
        description=f"Dados de geoquimica de {classe}",
        tags=["geoquimica", "postgresql", "ESRI"],
        default_args=default_args
    )
    def analise_quimica_etl():
        """
        Geoquímica: Contagem de Pintas de Au
        """
        # Argumentos para rodar o pipeline
        src_table_name = f"resultados_analiticos_{classe}"
        src_schema = "geoq_valida"
        src_primary_key = "objectid"

        # Coordenadas
        src_x_field = "longitude"
        src_y_field = "latitude"
        src_srid = 4674

        # Temporal
        src_ts_field = "data_de_analise"
        src_ts_format = "%d/%m/%Y"

        # Destino
        dst_schema = "geoquimica"
        dst_survey_table_name = f"{classe}_amostras"
        dst_assay_table_name = f"{classe}_analises"
        dst_assay_index_names=("amostra", "analito")
        dst_assay_value_name="quantidade"
        dst_assay_excluded_columns = ("globalid", "lote", "ra", "metodo", "created_user", "created_date", "last_edited_user", "last_edited_date")
        
        # Views materializadas
        dst_sde_schema = 'arcgis'
        dst_sde_matviews = (
            (dst_sde_schema, 'geoquimica_{classe}_amostras'), 
            (dst_sde_schema, 'geoquimica_{classe}_analises'), 
            (dst_sde_schema, 'geoquimica_{classe}_pivot_join'), 
        )

        @task(multiple_outputs=True)
        def extract_samples_table(**kwargs):
            # Leitura do banco
            conn_name = Variable.get("GEOQUIMICA_SRC_DATABASE", default_var="geoq_valida")
            hook = PostgresHook(postgres_conn_id=conn_name)

            try:
                log.info("Extraindo dados da tabela %s.%s", src_schema, src_table_name)
                sample_df = (
                    hook.get_pandas_df(f"SELECT * FROM {src_schema}.{src_table_name}", index_col=src_primary_key)
                        # Sanitizar colunas, para não inserir valores fora de padrão na DDL do banco (acentos, caracteres especiais, etc)
                        .rename(columns=lambda col: slugify(col, separator="_"))
                )
                
            except Exception as e:
                log.error(e)
                raise e

            assert not sample_df.empty, "Esta tabela está vazia. Abortando..."
            assert sample_df.index.is_unique, f"Esta tabela não possui identifcador único de registro no campo <{src_primary_key}>"

            dataset = export_parquet(sample_df, "contagem_pintas_au/bronze", f"{src_schema}_{src_table_name}.parquet")

            # recuperar colunas das tabelas de destino
            conn_name = Variable.get("GEOSGB_GOLD_CONNECTION", default_var="geosgb_gold")
            hook = PostgresHook(postgres_conn_id=conn_name)

            survey_cols = get_postgres_table_colums(hook, dst_schema, dst_survey_table_name) 
            assay_cols = tuple(filter(lambda col: col not in survey_cols + dst_assay_excluded_columns, sample_df.columns))

            return {
                "dataset": dataset,
                "survey_cols": survey_cols,
                "assay_cols": assay_cols
            }


        @task()
        def sanitize_survey_dataset(dataset, survey_cols, **kwargs):
            import geopandas as gpd
            import pandas as pd

            # Ler dados bronze
            df = pd.read_parquet(dataset)        

            # Tratamento de dados de amostras
            # 1- Transformar dataframe em geodataframe
            survey_df = gpd.GeoDataFrame(
                df.filter(survey_cols)
                    .apply(lambda col: col.replace("", None))
                    .rename(columns=lambda col: slugify(col, separator="_"))    
                    .assign(
                        # forçar duplicata como booleano
                        duplicata=lambda df: df.duplicata.fillna("não").str.match("(sim|1)", case=False)
                    ),
                geometry=gpd.points_from_xy(
                    df[src_x_field], 
                    df[src_y_field], 
                    crs=src_srid
                )
            )

            # Consertar campos de timestamp
            if src_ts_field in survey_df.columns:
                data_analise_ser = pd.to_datetime(survey_df[src_ts_field], format=src_ts_format, errors='coerce')

                # Tratamento de datas inválidas
                # date_invalid_idx = (
                #     survey_df[[src_ts_field]]
                #         .join(
                #             data_analise_ser,
                #             rsuffix='_converted'
                #         )
                #         .loc[
                #             lambda df:df.data_de_analise_converted.isna()
                #         ].index
                # )
                
                # if not date_invalid_idx.empty:
                #     out_date_invalid_file = create_filename(src_schema, src_table_name, "parquet", table_item, suffix="survey_date_invalid")
                #     survey_df.loc[date_invalid_idx].to_parquet(out_date_invalid_file, index=True)
                
                survey_df[src_ts_field] = data_analise_ser
                    
                del data_analise_ser
            
            # ObjectID tem que ser único e não pode ter geometria nula ou vazia
            assert survey_df.index.is_unique, f"ObjectID precisa ser único: {survey_df[survey_df.index.duplicated()].index.tolist()}"
            assert not (survey_df.geometry.isna().all() and survey_df.geometry.is_empty.all()), "A tabela não pode ter geometria nula ou vazia"

            return export_parquet(survey_df, "contagem_pintas_au/silver", f"{dst_schema}_{dst_survey_table_name}.parquet")
        

        @task()
        def sanitize_assay_dataset(dataset, assay_cols, **kwargs):
            import pandas as pd

            spaces_regex = r"\s"
            missing_values = [
                "ND", "", "N.A.", 0, "0", " ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", 
                "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", 
                "NULL", "NaN", "None", "n/a", "nan", "null"
            ]

            humanized_columns = {
                "ouro_0_5_mm": "ouro (<0,5mm)",
                "ouro_0_5_1_mm": "ouro (0,5 a 1mm)",
                "ouro_1_mm": "ouro (>1mm)"
            }

            # Pipes
            def handle_missing(series, extra_missing_values=[]):
                return (
                    series.str.replace(spaces_regex, "", regex=True)
                        .replace(missing_values + extra_missing_values, None) 
                )

            # def handle_normalized(series, replaces={}):
            #     for key, value in replaces.items():
            #         series = series.str.replace(key, value)
            #     return series

            # Ler dados bronze
            df = pd.read_parquet(dataset)        

            # Tratamento de dados de amostras
            assay_df = (
                df.filter(assay_cols)
                    .apply(lambda col: col.replace("", None))
                    .rename(columns=lambda col: slugify(col, separator="_"))
                    # Traz o objectid para o index do dataframe
                    # .set_index(assay_meta, append=True)
                    # Humaniza os nomes
                    .rename(columns=humanized_columns)
                    # De-pivot
                    .stack()
                    # Ajusta nomes
                    .rename_axis(dst_assay_index_names)
                    .rename(dst_assay_value_name)
                    # handle missing data on values
                    .pipe(handle_missing)
                    .dropna()
                    # # normalize values  qualificators
                    # .pipe(handle_normalized, normalize_values)
            )

            # ObjectID tem que ser único
            assert assay_df.index.is_unique

            # Valores precisam atender a padrão de valores
            values_match = assay_df.astype(str).str.match(r"^\d+$")

            # try:
            assert values_match.all(), f"Alguns valores <{assay_df[~values_match].shape[0]}> não coincidiram com a expressão regular: \n{assay_df[~values_match].head()}"
                
            # except AssertionError as e:
            #     log.warning(e)
            #     assay_error_oids = assay_df[~values_match].index.get_level_values(0).drop_duplicates().tolist()

            #     # Write errors
            #     out_assay_error_file = create_filename(src_schema, src_table_name, "csv", table_item, suffix="assay_errors")
            #     df[df.index.isin(assay_error_oids)].to_csv(out_assay_error_file, index=True)
            #     logging.warn(f"Amostras com problemas de validação de valores estão salvas no arquivo '{out_assay_error_file}'")
            
            return export_parquet(assay_df.to_frame(), "contagem_pintas_au/silver", f"{dst_schema}_{dst_assay_table_name}.parquet")
            

        @task()
        def write_postgres(survey_file, assay_file, survey_chunksize=5000, assay_chunksize=10000, **kwargs):
            import geopandas as gpd
            import pandas as pd


            conn_name = Variable.get("GEOSGB_GOLD_CONNECTION", default_var="geosgb_gold")
            hook = PostgresHook(postgres_conn_id=conn_name)

            # Gravar o parquet no banco de dados
            with hook.get_sqlalchemy_engine().connect() as conn:
                with conn.begin():
                    # Truncar tabelas
                    logging.info("Truncando tabelas...")
                    conn.execute(
                        text('TRUNCATE TABLE %s.%s CASCADE;' % (dst_schema, dst_survey_table_name))
                    )
                    
                    # 'to_postgis' não funciona com method. Ver como fazer isso com 'to_sql' tradicional
                    logging.info("Gravando amostras...")
                    ( 
                    gpd.read_parquet(survey_file)
                        .to_postgis(
                            dst_survey_table_name, 
                            conn, 
                            if_exists='append', 
                            schema=dst_schema, 
                            index=True, 
                            chunksize=survey_chunksize
                        )
                    )

                    logging.info("Gravando análises...")
                    (
                    pd.read_parquet(assay_file)
                        .reset_index()
                        .rename_axis("id")
                        .to_sql(
                            dst_assay_table_name, 
                            conn, 
                            if_exists='append', 
                            schema=dst_schema, 
                            index=False, 
                            chunksize=assay_chunksize, 
                            # method="multi" # https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method
                        )
                    )

                # Refresh MatView
                with conn.begin():                
                    log.info("Atualizando MatViews...")
                    
                    for schema, matview in dst_sde_matviews:
                        conn.execute(text("SET max_parallel_workers_per_gather = 0;"))
                        conn.execute(text(f'REFRESH MATERIALIZED VIEW {schema}.{matview};'))            
                        conn.execute(text("RESET max_parallel_workers_per_gather;"))            
                        conn.execute(text(f"REINDEX TABLE {schema}.{matview};"))            
                        conn.execute(text(f"ANALYZE {schema}.{matview};"))
                    
                log.info("Finalizado!")

        
        # Instantiate tasks
        results = extract_samples_table()
        # survey_sanitized = sanitize_survey_dataset(results["dataset"], results["survey_cols"])
        # assay_sanitized = sanitize_assay_dataset(results["dataset"], results["assay_cols"])
        # write_postgres(survey_sanitized, assay_sanitized)

        
    _dag = analise_quimica_etl()

if __name__ == "__main__":
    _dag.test()
