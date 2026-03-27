import logging

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from teste.geoquimica.models import GeoquimicaETLConfig


log = logging.getLogger("airflow.task")


@task()
def write_postgres(survey_file, assay_file, etl: GeoquimicaETLConfig, survey_chunksize=5000, assay_chunksize=10000, **kwargs):
    import geopandas as gpd
    import pandas as pd
    from sqlalchemy import text


    conn_name = Variable.get("GEOSGB_GOLD_CONNECTION", default_var="geosgb_gold")
    hook = PostgresHook(postgres_conn_id=conn_name)

    # Gravar o parquet no banco de dados
    with hook.get_sqlalchemy_engine().connect() as conn:
        with conn.begin():
            # Truncar tabelas
            logging.info("Truncando tabelas...")
            conn.execute(
                text('TRUNCATE TABLE %s.%s CASCADE;' % (etl.destination.schema, etl.destination.surveyTable.name))
            )
            
            # 'to_postgis' não funciona com method. Ver como fazer isso com 'to_sql' tradicional
            logging.info("Gravando amostras...")
            ( 
            gpd.read_parquet(survey_file)
                .to_postgis(
                    etl.destination.surveyTable.name, 
                    conn, 
                    if_exists='append', 
                    schema=etl.destination.schema, 
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
                    etl.destination.assayTable.name, 
                    conn, 
                    if_exists='append', 
                    schema=etl.destination.schema, 
                    index=False, 
                    chunksize=assay_chunksize, 
                    # method="multi" # https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method
                )
            )

        # Refresh MatView ()
        with conn.begin():
            log.info("Atualizando MatViews...")
            
            for item in etl.destination.matViews:
                try:
                    schema, matview = item.split(".")
                except:
                    schema = "public"
                    matview = item
                
                conn.execute(text("SET max_parallel_workers_per_gather = 0;"))
                conn.execute(text(f'REFRESH MATERIALIZED VIEW {schema}.{matview};'))            
                conn.execute(text("RESET max_parallel_workers_per_gather;"))
                conn.execute(text(f"REINDEX TABLE {schema}.{matview};"))            
                conn.execute(text(f"ANALYZE {schema}.{matview};"))
            
        log.info("Finalizado!")