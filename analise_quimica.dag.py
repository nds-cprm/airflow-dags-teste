import logging


# from airflow.datasets import Dataset, DatasetAlias
from airflow.decorators import dag
# from airflow.exceptions import AirflowFailException, AirflowSkipException
# from airflow.providers.smtp.operators.smtp import EmailOperator
from pathlib import Path

from teste.common import default_args
from teste.geoquimica.models import GeoquimicaETLConfig
from teste.geoquimica.tasks.common import extract_bronze_table, sanitize_survey_dataset
from teste.geoquimica.tasks.load import write_postgres
from teste.geoquimica.tasks.analise_quimica import sanitize_assay_dataset


log = logging.getLogger("airflow.task")

etl_conf_dir = Path(__file__).parent


for file_path in etl_conf_dir.glob('analise_quimica_*.yaml'):
    etl_conf = GeoquimicaETLConfig.from_yaml(file_path)

    @dag(
        dag_id=etl_conf.name,
        schedule=etl_conf.schedule,
        description=etl_conf.description,
        tags=etl_conf.tags,
        default_args=default_args,
        **etl_conf.dagArgs
    )
    def analise_quimica_etl():
        # Instantiate tasks
        results = extract_bronze_table(etl_conf)
        survey_sanitized = sanitize_survey_dataset(results["dataset"], results["survey_cols"], etl_conf)
        assay_sanitized = sanitize_assay_dataset(results["dataset"], results["assay_cols"], etl_conf)
        write_postgres(survey_sanitized, assay_sanitized, etl_conf)

        
    _dag = analise_quimica_etl()
    globals()[etl_conf.name] = _dag


if __name__ == "__main__":
    globals()[etl_conf.name].test()
