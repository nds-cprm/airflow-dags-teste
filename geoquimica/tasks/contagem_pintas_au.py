import logging

from airflow.decorators import task
from slugify import slugify

from sgb.common import export_parquet
from sgb.geoquimica.models import GeoquimicaETLConfig


log = logging.getLogger("airflow.task")


@task()
def sanitize_assay_dataset(dataset, assay_cols, etl: GeoquimicaETLConfig, **kwargs):
    import pandas as pd

    # Data cleaning constants
    spaces_regex = r"\s"

    missing_values = [
        "ND", "", "N.A.", 0, "0", " ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", 
        "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", 
        "NULL", "NaN", "None", "n/a", "nan", "null"
    ]

    humanized_columns = {
        "ouro_0_5_mm": "ouro (<0,5mm)",
        "ouro_0_5_1_mm": "ouro (0,5 a 1mm)",
        "ouro_1_mm": "ouro (>1mm)"
    }

    # Pipes
    def handle_missing(series, extra_missing_values=[]):
        return (
            series.str.replace(spaces_regex, "", regex=True)
                .replace(missing_values + extra_missing_values, None) 
        )

    # def handle_normalized(series, replaces={}):
    #     for key, value in replaces.items():
    #         series = series.str.replace(key, value)
    #     return series

    # Ler dados bronze
    df = pd.read_parquet(dataset)        

    # Tratamento de dados de amostras
    valueColumn = etl.destination.assayTable.valueColumn

    if isinstance(valueColumn, tuple):
        valueColumn = "_".join(valueColumn)

    assay_df = (
        df.filter(assay_cols)
            .apply(lambda col: col.replace("", None))
            .rename(columns=lambda col: slugify(col, separator="_"))
            # Traz o objectid para o index do dataframe
            # .set_index(assay_meta, append=True)
            # Humaniza os nomes
            .rename(columns=humanized_columns)
            # De-pivot
            .stack()
            # Ajusta nomes
            .rename_axis(etl.destination.assayTable.indexColumns)
            .rename(valueColumn)
            # handle missing data on values
            .pipe(handle_missing)
            .dropna()
            # # normalize values  qualificators
            # .pipe(handle_normalized, normalize_values)
    )

    # Index tem que ser único
    assert assay_df.index.is_unique

    # Valores precisam atender a padrão de valores
    values_match = assay_df.astype(str).str.match(r"^\d+$")

    # try:
    assert values_match.all(), f"Alguns valores <{assay_df[~values_match].shape[0]}> não coincidiram com a expressão regular: \n{assay_df[~values_match].head()}"
        
    # except AssertionError as e:
    #     log.warning(e)
    #     assay_error_oids = assay_df[~values_match].index.get_level_values(0).drop_duplicates().tolist()

    #     # Write errors
    #     out_assay_error_file = create_filename(src_schema, src_table_name, "csv", table_item, suffix="assay_errors")
    #     df[df.index.isin(assay_error_oids)].to_csv(out_assay_error_file, index=True)
    #     logging.warn(f"Amostras com problemas de validação de valores estão salvas no arquivo '{out_assay_error_file}'")
    
    # Gravar em Parquet
    return export_parquet(
        assay_df.to_frame(), 
        f"{etl.name}/silver", 
        f"{etl.destination.schema}_{etl.destination.assayTable.name}.parquet"
    )
