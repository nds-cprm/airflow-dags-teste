import logging

from airflow.decorators import dag
# from airflow.exceptions import AirflowFailException, AirflowSkipException
# from airflow.providers.smtp.operators.smtp import EmailOperator
from pathlib import Path

from sgb.common import default_args
from sgb.geoquimica.models import GeoquimicaETLConfig
from sgb.geoquimica.tasks.common import extract_bronze_table, sanitize_survey_dataset
from sgb.geoquimica.tasks.contagem_pintas_au import sanitize_assay_dataset
from sgb.geoquimica.tasks.load import write_postgres


log = logging.getLogger("airflow.task")

etl_conf = GeoquimicaETLConfig.from_yaml(Path(__file__).parent.joinpath("contagem_pintas_au.yaml"))


@dag(
    dag_id=etl_conf.name,
    schedule=etl_conf.schedule,
    description=etl_conf.description,
    tags=etl_conf.tags,
    default_args=default_args,
    **etl_conf.dagArgs
)
def contagem_de_pintas_au_etl():
    """
    Geoquímica: Contagem de Pintas de Au
    """
    results = extract_bronze_table(etl_conf)
    survey_sanitized = sanitize_survey_dataset(results["dataset"], results["survey_cols"], etl_conf)
    assay_sanitized = sanitize_assay_dataset(results["dataset"], results["assay_cols"], etl_conf)
    write_postgres(survey_sanitized, assay_sanitized, etl_conf)

    
_dag = contagem_de_pintas_au_etl()

if __name__ == "__main__":
    _dag.test()
