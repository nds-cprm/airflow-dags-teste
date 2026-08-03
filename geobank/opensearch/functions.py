import logging
import pyarrow.parquet as pq
import shapely.wkb
import shapely.geometry

from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime
from pathlib import Path
from opensearchpy.helpers import bulk

from .templates import ID_COLUMN, GEOMETRY_COLUMN, INDEX_BODY_TEMPLATE


logger = logging.getLogger("airflow.task")


# Função auxiliar para instanciar o cliente OpenSearch otimizado
def _get_opensearch_client(log_query=True):
    conn_id = Variable.get("GEOSGB_OPENSEARCH_CONNECTION", "opensearch_default")
    os_hook = OpenSearchHook(open_search_conn_id=conn_id, log_query=log_query)
    return os_hook.client


# 1. Função de Python para garantir a existência do Alias
def bootstrap_alias_logic(alias_name):
    client = _get_opensearch_client()

    if not client.indices.exists_alias(name=alias_name):
        logger.info(f"Alias '{alias_name}' não encontrado. Iniciando bootstrap...")
        initial_index = "spatial_index_bootstrap"

        if not client.indices.exists(index=initial_index):
            client.indices.create(index=initial_index, body=INDEX_BODY_TEMPLATE)

        client.indices.update_aliases(
            body={
                "actions": [{"add": {"index": initial_index, "alias": alias_name}}]
            }
        )

    else:
        logger.warning(f"Alias '{alias_name}' já existe.")


# 2. Função de Python para gerar e retornar o nome do índice com Timestamp
def generate_index_name_logic(alias_name):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_index_name = f"{alias_name}_{timestamp}"

    return new_index_name  # O retorno via PythonOperator é automaticamente salvo no XCom


# 3. Função de Python para realizar a carga pesada via PyArrow
def stream_heavy_spatial_load_logic(**kwargs):
    ti = kwargs["ti"]
    PARQUET_FILE_PATH = Path(PARQUET_FILE_PATH)

    # Recupera o nome do índice gerado pela tarefa anterior via XCom
    target_index_name = ti.xcom_pull(task_ids="generate_new_index_name")
    client = _get_opensearch_client()

    if not PARQUET_FILE_PATH:
        raise FileNotFoundError(f"Arquivo GeoParquet não encontrado: {PARQUET_FILE_PATH}")

    parquet_file = pq.ParquetFile(PARQUET_FILE_PATH)

    def generate_heavy_spatial_actions():
        for i in range(parquet_file.num_row_groups):
            row_group = parquet_file.read_row_group(i)
            records = row_group.to_pylist()

            for row in records:
                doc_id = str(row.get(ID_COLUMN))
                wkb_bytes = row.pop(GEOMETRY_COLUMN)

                # Remove coluna injetada pelo Arrow
                # row.pop("__index_level_0__")

                # TODO: Remove from parquet
                # row.pop("shape.area")
                # row.pop("shape.len")

                if not wkb_bytes or not doc_id:
                    continue

                try:
                    geom = shapely.wkb.loads(wkb_bytes)
                    geojson_geometry = shapely.geometry.mapping(geom)
                    # row[GEOMETRY_COLUMN] = geojson_geometry

                except AirflowException as geom_err:
                    logger.error(f"Erro na geometria do ID {doc_id}: {geom_err}")
                    continue

                yield {
                    "_op_type": "index",
                    "_index": target_index_name,
                    "_id": doc_id,
                    "_source": {
                        "type": "Feature",
                        GEOMETRY_COLUMN: geojson_geometry, 
                        "properties": row
                    }
                }

    logger.info(f"Iniciando a transmissão dos dados no índice {target_index_name}...")

    success_count, errors = bulk(
        client=client,
        actions=generate_heavy_spatial_actions(),
        chunk_size=0,
        max_chunk_bytes=10 * 1024 * 1024,
        raise_on_error=False,
        request_timeout=60
    )

    logger.info(f"Sucesso: {success_count} polígonos indexados.")

    if errors:
        raise AirflowException("Pipeline de carga falhou parcialmente.")
    

# 4. Função de Python para chavear o Alias e limpar índices órfãos
def atomic_alias_switch_and_cleanup_logic(alias_name, **kwargs):
    ti = kwargs["ti"]

    # Recupera o nome do índice que acabou de ser populado
    new_index_name = ti.xcom_pull(task_ids="generate_new_index_name")
    client = _get_opensearch_client()
    
    old_indices = []

    if client.indices.exists_alias(name=alias_name):
        alias_info = client.indices.get_alias(name=alias_name)
        old_indices = list(alias_info.keys())

    actions = [{"add": {"index": new_index_name, "alias": alias_name}}]
    
    for old_index in old_indices:
        actions.append({"remove": {"index": old_index, "alias": alias_name}})

    logger.info(f"Executando troca atômica do Alias para apontar para {new_index_name}...")

    client.indices.update_aliases(body={"actions": actions})

    for old_index in old_indices:
        if old_index != new_index_name:
            logger.warning(f"Removendo índice legado obsoleto: {old_index}")
            client.indices.delete(index=old_index) #, ignore=)
