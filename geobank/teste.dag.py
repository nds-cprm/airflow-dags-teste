from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from collections import namedtuple
from pathlib import Path

# from sgb.common.opensearch.operators import TemplatedOpenSearchCreateIndexOperator
# from sgb.common.opensearch.functions import (
#     bootstrap_alias_logic, 
#     generate_index_name_logic, 
#     stream_heavy_spatial_load_logic, 
#     atomic_alias_switch_and_cleanup_logic
# )
# from sgb.common.opensearch.templates import INDEX_BODY_TEMPLATE
from sgb.common.sql.functions import read_sql_from_file 
from sgb.common.oracle.functions import get_bronze_database, extract_sql_query_to_gpkg # , sanitize_data


Table = namedtuple("Table", ["name", "sql"])
FeatureClass = namedtuple("FeatureClass", ["name", "sql", "parameters"])


ROOT_DIR = Path(__file__).parent


dataset_name = "litoestratigrafia"

related_tables = [
    Table(name="litologia", sql="SELECT OBJECTID AS ID, ID_UNIDADE_ESTRATIGRAFICA, ROCHA, CLASSE, PREDOMINANCIA "
                                "FROM LITOESTRATIGRAFIA.MV_LITOLOGIA"),
    Table(name="datacao", sql="SELECT OBJECTID AS ID, ID_UNIDADE_ESTRATIGRAFICA, DATA_CADASTRO, IDADE_MIN, ERRO_IDADE_MIN, "
                              "QUALIDADE_INFER_IDADE_MIN, EON_MIN, ERA_MIN, SISTEMA_MIN, EPOCA_MIN, SISTEMA_GEOCRON_MIN, "
                              "METODO_GEOCRON_MIN, IDADE_MAX, ERRO_IDADE_MAX, QUALIDADE_INFER_IDADE_MAX, EON_MAX, ERA_MAX, "
                              "SISTEMA_MAX, EPOCA_MAX, SISTEMA_GEOCRON_MAX, METODO_GEOCRON_MAX FROM LITOESTRATIGRAFIA.MV_DATACOES"),
    Table(name="metamorfismo", sql="SELECT OBJECTID AS ID, ID_UNIDADE_ESTRATIGRAFICA, FACIE_METAMORFICA, METODO_GEOTERMOBAROMETRICO, "
                                   "TIPO_BARICO, NIVEL_CRUSTAL, GRAU_METAMORFICO, TIPO_METAMORFISMO, ZONA_FALHA, CISALHAMENTO, "
                                   "TRAJETORIA_PTT, TEMPERATURA_PICO, ERRO_TEMPERATURA_PICO, PRESSAO_PICO, ERRO_PRESSAO_PICO, IDADE_MAX, "
                                   "ERRO_IDADE_MAX, METODO_GEOCRON_MAX, SISTEMA_GEOCRON_MAX, QUALIDADE_INFER_IDADE_MAX, IDADE_MIN, "
                                   "ERRO_IDADE_MIN, METODO_GEOCRON_MIN, SISTEMA_GEOCRON_MIN, QUALIDADE_INFER_IDADE_MIN "
                                   "FROM LITOESTRATIGRAFIA.MV_METAMORFISMOS"),
    Table(name="magmatismo", sql="SELECT OBJECTID AS ID, ID_UNIDADE_ESTRATIGRAFICA, ASSOCIACAO_MAGMATICA, TEXTURA_IGNEA, NIVEL_CRUSTAL, "
                                 "FONTE_MAGMATICA, MORFOLOGIA_INTRUSAO, IDADE_SM_ND_MEDIDA, IDADE_SM_ND_CALCULADA "
                                 "FROM LITOESTRATIGRAFIA.MV_MAGMATISMOS"),
    Table(name="ambiente_sedimentar", sql="SELECT OBJECTID AS ID, ID_UNIDADE_ESTRATIGRAFICA, AMBIENTE_DEPOSICIONAL, SISTEMA_DEPOSICIONAL, "
                                          "DEPOSITO_SEDIMENTAR, ESTRUTURA_SEDIMENTAR FROM LITOESTRATIGRAFIA.MV_AMBIENTE_SEDIMENTAR"),
    Table(name="escala", sql="SELECT OBJECTID AS ID, ID_UNIDADE_ESTRATIGRAFICA, ESCALA FROM LITOESTRATIGRAFIA.MV_ESCALAS"),
    # Table(name="cores", sql=read_sql_from_file(ROOT_DIR / "oracle/sql/related/litoestratigrafia_cores.sql")),
    Table(name="siglas_anteriores", sql=read_sql_from_file(ROOT_DIR / "oracle/sql/related/litoestratigrafia_siglas_anteriores.sql")),
]

feature_classes = [
    FeatureClass(
        name="litoestratigrafia_2500000", 
        sql=read_sql_from_file(ROOT_DIR / "oracle/sql/main/litoestratigrafia_2.5M.sql"),
        parameters={},
    ),
    FeatureClass(
        name="litoestratigrafia_1000000", 
        sql=read_sql_from_file(ROOT_DIR / "oracle/sql/main/litoestratigrafia_1M.sql"),
        parameters={},
    ),
    # FeatureClass(
    #     name="litoestratigrafia_estados", 
    #     sql=read_sql_from_file(ROOT_DIR / "oracle/sql/main/litoestratigrafia_estados.sql"),
    #     parameters={},
    # ),
]

# multi-escala
for scale, key in {'250000': 4, '100000': 5, '150000': 17, '50000': 6, '25000': 7}.items():
    feature_classes.append(
        FeatureClass(
            name=f"litoestratigrafia_{scale}", 
            sql=read_sql_from_file(ROOT_DIR / "oracle/sql/main/litoestratigrafia_escalas.sql"),
            parameters={"p_escala": key},
        )
    )


# Declaração clássica da DAG
with DAG(
    dag_id=f"geobank_{dataset_name}",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["opensearch", "teste", "oracle"]
) as dag:
    # repositório bronze, em geopackage
    task_get_bronze_database = PythonOperator(
        task_id="get_bronze_database",
        python_callable=get_bronze_database,
        op_args=[dataset_name],
    )
    
    # Tabelas relacionadas
    task_empty_after_related = EmptyOperator(
        task_id="empty_after_related",
    )
    
    with TaskGroup(group_id="extract_related_tables") as extract_related_tables:
        for table in related_tables:
            task_extract_related = PythonOperator(
                task_id=f"extract_related_{table.name}",
                python_callable=extract_sql_query_to_gpkg,
                op_kwargs={
                    "gpkg": "{{ ti.xcom_pull(task_ids='get_bronze_database') }}", 
                    "layer_name": table.name,
                    "sql": table.sql,
                    "pk": "ID", 
                }
            )

    task_get_bronze_database >> extract_related_tables >> task_empty_after_related

    # Tabelas principais
    task_empty_after_features = EmptyOperator(
        task_id="empty_after_features",
        trigger_rule="one_success"
    )

    for feature in feature_classes:
        with TaskGroup(group_id=f"data_process_{feature.name}") as data_processing:
            task_extract_features = PythonOperator(
                task_id=f"extract_feature_{feature.name}",
                python_callable=extract_sql_query_to_gpkg,
                op_kwargs={
                    "gpkg": "{{ ti.xcom_pull(task_ids='get_bronze_database') }}", 
                    "layer_name": feature.name,
                    "sql": feature.sql,
                    "pk": "FID", 
                    "geometry_col": "GEOMETRY",
                    "parameters": feature.parameters
                }
            )

        task_empty_after_related >> data_processing >> task_empty_after_features

    

    # task_sanitize_data = PythonOperator(
    #     task_id="sanitize_data",
    #     python_callable=sanitize_data
    # )

    # task_bootstrap_alias = PythonOperator(
    #     task_id="bootstrap_alias_if_not_exists",
    #     python_callable=bootstrap_alias_logic,
    #     op_args=["geosgb_litoestratigrafia_2500k"]
    # )

    # task_generate_name = PythonOperator(
    #     task_id="generate_new_index_name",
    #     python_callable=generate_index_name_logic,
    #     op_args=["geosgb_litoestratigrafia_2500k"]
    # )

    # # Uso do operador oficial do Provedor OpenSearch renderizando o XCom via Jinja Template
    # task_create_index = TemplatedOpenSearchCreateIndexOperator(
    #     task_id="create_optimized_spatial_index",
    #     opensearch_conn_id="{{ var.value.GEOBANK_OPENSEARCH_CONN_ID }}",
    #     index_name="{{ ti.xcom_pull(task_ids='generate_new_index_name') }}",
    #     index_body=INDEX_BODY_TEMPLATE,
    # )

    # task_stream_load = PythonOperator(
    #     task_id="stream_heavy_spatial_load",
    #     python_callable=stream_heavy_spatial_load_logic,
    # )

    # task_switch_cleanup = PythonOperator(
    #     task_id="atomic_alias_switch_and_cleanup",
    #     python_callable=atomic_alias_switch_and_cleanup_logic,
    # )

    # Fluxo clássico de orquestração por bitshift (>>) 
    # task_extract_feature_layer >> task_sanitize_data

    # task_bootstrap_alias >> task_generate_name >> task_create_index 
    # [task_sanitize_data, task_create_index] >> task_stream_load >> task_switch_cleanup
    
