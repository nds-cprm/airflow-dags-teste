from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from sgb.geobank.opensearch.operators import TemplatedOpenSearchCreateIndexOperator
from sgb.geobank.opensearch.functions import (
    bootstrap_alias_logic, 
    generate_index_name_logic, 
    stream_heavy_spatial_load_logic, 
    atomic_alias_switch_and_cleanup_logic
)
from sgb.geobank.opensearch.templates import INDEX_BODY_TEMPLATE
from sgb.geobank.oracle.functions import extract_data_from_oracle


# ALIAS_NAME = "litoestratigrafia_1m" 
# PARQUET_FILE_PATH = "/home/mota/vscode/airflow/airflow-spatial/data/tmp/litoestratigrafia_1000000/silver/litoestratigrafia_1000000_new.parquet"


# Declaração clássica da DAG
with DAG(
    dag_id="opensearch_3_7_operators_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["opensearch", "teste"]
) as dag:
    task_extract_from_oracle = PythonOperator(
        task_id="extract_from_oracle",
        python_callable=extract_data_from_oracle
    )

    # task_bootstrap_alias = PythonOperator(
    #     task_id="bootstrap_alias_if_not_exists",
    #     python_callable=bootstrap_alias_logic,
    # )

    # task_generate_name = PythonOperator(
    #     task_id="generate_new_index_name",
    #     python_callable=generate_index_name_logic,
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
    (
        # task_bootstrap_alias 
        # >> task_generate_name 
        # >> task_create_index 
        # >> task_stream_load 
        # >> task_switch_cleanup
    )
