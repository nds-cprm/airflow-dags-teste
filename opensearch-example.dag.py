import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook
from airflow.providers.opensearch.operators.opensearch import OpenSearchCreateIndexOperator
import pyarrow.parquet as pq
import shapely.wkb
import shapely.geometry
# from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk

CONN_ID = "opensearch_default"
ALIAS_NAME = "litoestratigrafia_1m" 
PARQUET_FILE_PATH = "/home/mota/vscode/airflow/airflow-spatial/data/tmp/litoestratigrafia_1000000/silver/litoestratigrafia_1000000_new.parquet"

ID_COLUMN = "fid"
GEOMETRY_COLUMN = "geometry"

# Configuração base de mapeamento e settings para o OpenSearch 3.7
INDEX_BODY_TEMPLATE = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "refresh_interval": "60s",  #  Se colocar o valor para -1, forçar a reindexação: POST geo-dados-v1/_refresh
            "translog": {
                "durability": "async",
                "sync_interval": "5s"
            }
        }
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "type": {
                "type": "keyword"
            },
            GEOMETRY_COLUMN: {
                "type": "geo_shape",
                "ignore_malformed": False,
                "ignore_z_value": True
            },
            "properties": {
                "properties": {
                    ID_COLUMN: {
                        "type": "integer"
                    },
                    "id_uni_estratigrafica": {
                        "type": "integer",
                    }, # Integer (0.0)
                    "sigla": {
                        "type": "keyword",
                    }, # String (0.0)
                    "hierarquia": {
                        "type": "keyword",
                    }, # String (0.0)
                    "nome": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, # String (0.0)
                    "amb_tecto": {
                        "type": "keyword",
                    }, # String (0.0)
                    "sub_amb_tectonico": {
                        "type": "keyword",
                    }, # String (0.0)
                    "sigla_pai": {
                        "type": "keyword",
                    }, # String (0.0)
                    "nome_pai": {
                        "type": "keyword",
                    }, # String (0.0)
                    "legenda": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, # String (0.0)
                    "escala": {
                        "type": "keyword",
                    }, # String (0.0)
                    "mapa": {
                        "type": "keyword",
                    }, # String (0.0)
                    "litotipos": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, # String (0.0)
                    "range": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, # String (0.0)
                    "idade_min": {
                        "type": "float",
                    }, # Real (0.0)
                    "idade_max": {
                        "type": "float",
                    }, # Real (0.0)
                    "eon_min": {
                        "type": "keyword",
                    }, # String (0.0)
                    "eon_max": {
                        "type": "keyword",
                    }, # String (0.0)
                    "era_min": {
                        "type": "keyword",
                    }, # String (0.0)
                    "era_max": {
                        "type": "keyword",
                    }, # String (0.0)
                    "sistema_min": {
                        "type": "keyword",
                    }, # String (0.0)
                    "sistema_max": {
                        "type": "keyword",
                    }, # String (0.0)
                    "epoca_min": {
                        "type": "keyword",
                    }, # String (0.0)
                    "epoca_max": {
                        "type": "keyword",
                    }, # String (0.0)
                    "siglas_historicas": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, # String (0.0)
                    "grupo": {
                        "type": "keyword",
                    }, # String (0.0)
                    "record_hash": {
                        "type": "text",
                    }, # String (0.0)
                }
            }
        }
    }
}

# Função auxiliar para instanciar o cliente OpenSearch otimizado
def _get_opensearch_client():
    os_hook = OpenSearchHook(open_search_conn_id=CONN_ID, log_query=True)
    return os_hook.client

# 1. Função de Python para garantir a existência do Alias
def bootstrap_alias_logic():
    client = _get_opensearch_client()

    if not client.indices.exists_alias(name=ALIAS_NAME):
        print(f"Alias '{ALIAS_NAME}' não encontrado. Iniciando bootstrap...")
        initial_index = "spatial_index_bootstrap"

        if not client.indices.exists(index=initial_index):
            client.indices.create(index=initial_index, body=INDEX_BODY_TEMPLATE)

        client.indices.update_aliases(
            body={
                "actions": [{"add": {"index": initial_index, "alias": ALIAS_NAME}}]
            }
        )

    else:
        print(f"Alias '{ALIAS_NAME}' já existe.")

# 2. Função de Python para gerar e retornar o nome do índice com Timestamp
def generate_index_name_logic():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_index_name = f"{ALIAS_NAME}_{timestamp}"

    return new_index_name  # O retorno via PythonOperator é automaticamente salvo no XCom

# 3. Função de Python para realizar a carga pesada via PyArrow
def stream_heavy_spatial_load_logic(ti):
    # Recupera o nome do índice gerado pela tarefa anterior via XCom
    target_index_name = ti.xcom_pull(task_ids="generate_new_index_name")
    client = _get_opensearch_client()

    if not os.path.exists(PARQUET_FILE_PATH):
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
                row.pop("__index_level_0__")

                # TODO: Remove from parquet
                row.pop("shape.area")
                row.pop("shape.len")

                if not wkb_bytes or not doc_id:
                    continue

                try:
                    geom = shapely.wkb.loads(wkb_bytes)
                    geojson_geometry = shapely.geometry.mapping(geom)
                    # row[GEOMETRY_COLUMN] = geojson_geometry

                except Exception as geom_err:
                    print(f"Erro na geometria do ID {doc_id}: {geom_err}")
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

    print(f"Iniciando a transmissão dos dados no índice {target_index_name}...")

    success_count, errors = bulk(
        client=client,
        actions=generate_heavy_spatial_actions(),
        chunk_size=0,
        max_chunk_bytes=10 * 1024 * 1024,
        raise_on_error=False,
        request_timeout=60
    )

    print(f"Sucesso: {success_count} polígonos indexados.")

    if errors:
        raise Exception("Pipeline de carga falhou parcialmente.")

# 4. Função de Python para chavear o Alias e limpar índices órfãos
def atomic_alias_switch_and_cleanup_logic(ti):
    # Recupera o nome do índice que acabou de ser populado
    new_index_name = ti.xcom_pull(task_ids="generate_new_index_name")
    client = _get_opensearch_client()
    
    old_indices = []

    if client.indices.exists_alias(name=ALIAS_NAME):
        alias_info = client.indices.get_alias(name=ALIAS_NAME)
        old_indices = list(alias_info.keys())

    actions = [{"add": {"index": new_index_name, "alias": ALIAS_NAME}}]
    
    for old_index in old_indices:
        actions.append({"remove": {"index": old_index, "alias": ALIAS_NAME}})

    print(f"Executando troca atômica do Alias para apontar para {new_index_name}...")

    client.indices.update_aliases(body={"actions": actions})

    for old_index in old_indices:
        if old_index != new_index_name:
            print(f"Removendo índice legado obsoleto: {old_index}")
            client.indices.delete(index=old_index) #, ignore=)


# Classe filha para permitir o campo index_name como templated
class TemplatedOpenSearchCreateIndexOperator(OpenSearchCreateIndexOperator):
    template_fields = OpenSearchCreateIndexOperator.template_fields + ("index_name",)


# Declaração clássica da DAG
with DAG(
    dag_id="opensearch_3_7_operators_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["opensearch", "teste"]
) as dag:

    task_bootstrap_alias = PythonOperator(
        task_id="bootstrap_alias_if_not_exists",
        python_callable=bootstrap_alias_logic,
    )

    task_generate_name = PythonOperator(
        task_id="generate_new_index_name",
        python_callable=generate_index_name_logic,
    )

    # Uso do operador oficial do Provedor OpenSearch renderizando o XCom via Jinja Template
    task_create_index = TemplatedOpenSearchCreateIndexOperator(
        task_id="create_optimized_spatial_index",
        opensearch_conn_id=CONN_ID,
        index_name="{{ ti.xcom_pull(task_ids='generate_new_index_name') }}",
        index_body=INDEX_BODY_TEMPLATE,
    )

    task_stream_load = PythonOperator(
        task_id="stream_heavy_spatial_load",
        python_callable=stream_heavy_spatial_load_logic,
    )

    task_switch_cleanup = PythonOperator(
        task_id="atomic_alias_switch_and_cleanup",
        python_callable=atomic_alias_switch_and_cleanup_logic,
    )

    # Fluxo clássico de orquestração por bitshift (>>)
    (
        task_bootstrap_alias 
        >> task_generate_name 
        >> task_create_index 
        >> task_stream_load 
        >> task_switch_cleanup
    )
