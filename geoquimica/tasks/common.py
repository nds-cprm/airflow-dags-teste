import logging

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from slugify import slugify

from teste.common import export_parquet
from teste.common.postgres import get_postgres_table_colums
from teste.geoquimica.models import GeoquimicaETLConfig


log = logging.getLogger("airflow.task")


@task(multiple_outputs=True)
def extract_bronze_table(etl: GeoquimicaETLConfig, **kwargs):
    # Leitura do banco
    conn_name = Variable.get("GEOQUIMICA_SRC_DATABASE", default_var="geoq_valida")
    hook = PostgresHook(postgres_conn_id=conn_name)

    # src table
    src_schema = etl.source.schema
    src_table = etl.source.table
    src_pk = etl.source.primaryKeyColumn

    try:

        log.info("Extraindo dados da tabela %s.%s", src_schema, src_table)
        sample_df = (
            hook.get_pandas_df(f"SELECT * FROM {src_schema}.{src_table}", index_col=src_pk)
                # Sanitizar colunas, para não inserir valores fora de padrão na DDL do banco (acentos, caracteres especiais, etc)
                .rename(columns=lambda col: slugify(col, separator="_"))
        )
        
    except Exception as e:
        log.error(e)
        raise e

    assert not sample_df.empty, "Esta tabela está vazia. Abortando..."
    assert sample_df.index.is_unique, f"Esta tabela não possui identifcador único de registro no campo <{src_pk}>"

    dataset = export_parquet(sample_df, f"{etl.name}/bronze", f"{src_schema}_{src_table}.parquet")

    # recuperar colunas das tabelas de destino
    conn_name = Variable.get("GEOSGB_GOLD_CONNECTION", default_var="geosgb_gold")
    hook = PostgresHook(postgres_conn_id=conn_name)

    # dst table
    dst_schema = etl.destination.schema
    dst_table = etl.destination.surveyTable.name
    dst_excluded = etl.destination.assayTable.excludedColumns

    survey_cols = get_postgres_table_colums(hook, dst_schema, dst_table) 
    assay_cols = tuple(filter(lambda col: col not in list(survey_cols) + list(dst_excluded), sample_df.columns))

    return {
        "dataset": dataset,
        "survey_cols": survey_cols,
        "assay_cols": assay_cols
    }


@task()
def sanitize_survey_dataset(dataset, survey_cols, etl: GeoquimicaETLConfig, **kwargs):
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
            df[etl.source.geometryColumn.xColumn], 
            df[etl.source.geometryColumn.yColumn], 
            crs=etl.source.geometryColumn.srid
        )
    )

    # Consertar campos de timestamp
    for ts_col in etl.source.timestampColumn:
        if ts_col.name in survey_df.columns:
            data_analise_ser = pd.to_datetime(survey_df[ts_col.name], format=ts_col.format, errors='coerce')

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
            
            survey_df[ts_col.name] = data_analise_ser
            
        del data_analise_ser
    
    # ObjectID tem que ser único e não pode ter geometria nula ou vazia
    assert survey_df.index.is_unique, f"ObjectID precisa ser único: {survey_df[survey_df.index.duplicated()].index.tolist()}"
    assert not (survey_df.geometry.isna().all() and survey_df.geometry.is_empty.all()), "A tabela não pode ter geometria nula ou vazia"

    return export_parquet(
        survey_df, 
        f"{etl.name}/silver", 
        f"{etl.destination.schema}_{etl.destination.surveyTable.name}.parquet"
    )
