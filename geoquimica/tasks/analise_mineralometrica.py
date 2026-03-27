import logging

from airflow.decorators import task
from slugify import slugify

from teste.common import export_parquet
from teste.geoquimica.models import GeoquimicaETLConfig
from teste.geoquimica.utils import (
    handle_normalized, handle_missing, NORMALIZE_VALUES
)


log = logging.getLogger("airflow.task")


@task
def sanitize_weight_dataset(dataset, weight_cols, etl: GeoquimicaETLConfig, **kwargs):
    import pandas as pd

    # Ler dados bronze
    df = pd.read_parquet(dataset) 

    # Primeiras limpezas
    weight_df = (
        df.filter(weight_cols)
            .apply(lambda col: col.replace("", None))
            .rename(columns=lambda col: slugify(col, separator="_"))
            # De-pivot
            .stack()
            # Ajusta nomes
            .rename_axis(etl.destination.assayTable.indexColumns)
            .rename(etl.destination.assayTable.valueColumn)
            # handle missing data on values
            .pipe(handle_missing)
            .dropna()
            # normalize values + qualificators
            .pipe(handle_normalized, NORMALIZE_VALUES)
            # converte para float (peso não tem qualificador)
            # .astype(float)   # 
            # .to_frame()
    )

    # checar se ele pode serv convertido para numeric (Talvez trocar para regex)
    cast_numeric_error = pd.to_numeric(weight_df, errors='coerce').isna()
    assert weight_df[cast_numeric_error].size == 0, "Campos com problemas de conversão de tipo: %s" % weight_df[cast_numeric_error]

    weight_df = weight_df[~cast_numeric_error].astype(float)

    # Index tem que ser único
    assert weight_df.index.is_unique, "Index precisa ser único"

    # Gravar em Parquet
    return export_parquet(
        weight_df, 
        f"{etl.name}/silver", 
        f"{etl.destination.schema}_weight.parquet"
    )


@task
def sanitize_assay_dataset(dataset, assay_cols, etl: GeoquimicaETLConfig, **kwargs):
    import pandas as pd

    # Primeiras limpezas
    # index_names = [col.name for col in (assay_sample_column, assay_analyte_column)]
    # value_name = assay_value_column.name

    # Ler dados bronze
    df = pd.read_parquet(dataset) 