from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# from sgb.common.opensearch.operators import TemplatedOpenSearchCreateIndexOperator
# from sgb.common.opensearch.functions import (
#     bootstrap_alias_logic, 
#     generate_index_name_logic, 
#     stream_heavy_spatial_load_logic, 
#     atomic_alias_switch_and_cleanup_logic
# )
# from sgb.common.opensearch.templates import INDEX_BODY_TEMPLATE

from sgb.siagas.functions import extract_data_from_siagas

# ALIAS_NAME = "litoestratigrafia_1m" 
# PARQUET_FILE_PATH = "/home/mota/vscode/airflow/airflow-spatial/data/tmp/litoestratigrafia_1000000/silver/litoestratigrafia_1000000_new.parquet"


# Declaração clássica da DAG
with DAG(
    dag_id="siagas",
    start_date=datetime(2026, 10, 15),
    schedule_interval= None, # "@daily",
    catchup=False,
    tags=["siagas", "sqlserver"]
) as dag:
    task_extract_from_siagas = PythonOperator(
        task_id="extract_from_siagas",
        python_callable=extract_data_from_siagas
    )