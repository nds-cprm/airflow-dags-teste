import logging

from airflow.decorators import task
from slugify import slugify

from sgb.common import export_parquet
from sgb.geoquimica.models import GeoquimicaETLConfig
from sgb.geoquimica.utils import (
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

    weight_df = (
        weight_df.astype(float)
            .to_frame()
    )

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

    # Tratamento de dados de amostras
    indexColumns = etl.destination.assayTable.indexColumns
    valueColumn = etl.destination.assayTable.valueColumn

    assert len(indexColumns) == 2, "destination.assayTable.indexColumns precisa ter comprimento 2, com as colunas de amostra, analito, nesta ordem"
    assert isinstance(valueColumn, str), "destination.assayTable.valueColumn [%s] precisa ser do tipo string" % valueColumn

    # normalizar
    extra_values = {
        "1" : "S1",
        "3" : "S3",
        "15": "S15",
        "40": "S40",
        "60": "S60",
        "85": "S85",
    }

    # Regex para validar valores
    semiquant_regex = "^S(1|3|15|40|60|85)$"
    qualit_regex = "^(X|Y|Z)$"
        
    # Primeiras limpezas
    assay_df = (
        df.filter(assay_cols)
            .apply(lambda col: col.replace("", None))
            .rename(columns=lambda col: slugify(col, separator="_"))
            # Traz o objectid para o index do dataframe
            # .set_index(assay_meta, append=True)
            # De-pivot
            .stack()
            # Ajusta nomes
            .rename_axis(indexColumns)
            .rename(valueColumn)
            # handle missing data on values
            .pipe(handle_missing)
            .dropna()
            # ajustar valores semiquantitativos e força o texto para maiusculas (é somente enums)
            .replace(extra_values.keys(), extra_values.values())
            .str.upper()
    )

    # ObjectID tem que ser único
    # assert survey_df.index.is_unique

    # Valores precisam atender aos padrões de valores
    values_match = assay_df.str.match(semiquant_regex) | assay_df.str.match(qualit_regex)

    # try:
    assert values_match.all(), f"Alguns valores <{assay_df[~values_match].shape[0]}> não coincidiram com a expressão regular: \n{assay_df[~values_match].head()}"

    # except AssertionError as e:
    #     logging.warning(e)
    #     assay_error_oids = assay_df[~values_match].index.get_level_values(0).drop_duplicates().tolist()

    #     # write errors
    #     out_assay_error_file = create_filename(src_schema, src_table_name, "csv", table_item, suffix="assay_errors")
    #     df[df.index.isin(assay_error_oids)].to_csv(out_assay_error_file, index=True)
    #     logging.warning(f"Amostras com problemas de validação de valores estão salvas no arquivo '{out_assay_error_file}'")
        

    # Gravar em Parquet
    return export_parquet(
        assay_df.to_frame(), 
        f"{etl.name}/silver", 
        f"{etl.destination.schema}_{etl.destination.assayTable.name}.parquet"
    )
