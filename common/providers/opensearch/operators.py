from airflow.providers.opensearch.operators.opensearch import OpenSearchCreateIndexOperator


# Classe filha para permitir o campo index_name como templated
class TemplatedOpenSearchCreateIndexOperator(OpenSearchCreateIndexOperator):
    template_fields = OpenSearchCreateIndexOperator.template_fields + ("index_name",)
