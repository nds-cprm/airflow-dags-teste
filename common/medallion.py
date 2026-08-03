from pathlib import Path
from airflow.models import Variable
from datetime import datetime


GEOSGB_TEMP_DIR = Variable.get("GEOSGB_TEMP_DIR", "/tmp")

def get_bronze_out_dir(dataset_name: str, parents: bool = True, exist_ok: bool = True) -> Path:
    temp_dir = Path(GEOSGB_TEMP_DIR) # / "bronze" / dataset_name / datetime.strftime("%Y") / datetime.strftime("%m") / datetime.strftime("%d") 

    if not temp_dir.exists():
        temp_dir.mkdir(parents=parents, exist_ok=exist_ok)

    return temp_dir


def get_silver_out_dir(dataset_name: str, parents: bool = True, exist_ok: bool = True) -> Path:
    temp_dir = Path(GEOSGB_TEMP_DIR) / "silver" / dataset_name

    if not temp_dir.exists():
        temp_dir.mkdir(parents=parents, exist_ok=exist_ok)

    return temp_dir
