import logging

from airflow.utils.dates import days_ago
from pathlib import Path
from pendulum import duration


log = logging.getLogger("airflow.task")


# Diretório de intercâmbio
SHARED_FOLDER = Path("/tmp/geosgb")

if not SHARED_FOLDER.exists():
    SHARED_FOLDER.mkdir(exist_ok=True, parents=True)

default_args = {
    'owner': 'digeop',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['carlos.mota@sgb.gov.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': duration(minutes=10),
}

def export_parquet(df, relpath, filename):
    out_file = SHARED_FOLDER.joinpath(relpath, filename)
    out_dir = out_file.parent

    if not out_dir.exists():
        out_dir.mkdir(exist_ok=True, parents=True)

    log.info("Exportando arquivo %s", out_file)

    df.to_parquet(out_file, index=True)
    log.info(df.info())

    return out_file.as_posix()