import logging

from airflow.decorators import task
from slugify import slugify

from teste.common import export_parquet
from teste.geoquimica.models import GeoquimicaETLConfig


log = logging.getLogger("airflow.task")


@task
def sanitize_assay_dataset(dataset, assay_cols, etl: GeoquimicaETLConfig, **kwargs):
    import pandas as pd

    # Data cleaning constants
    spaces_regex = r"\s+"
    values_match_regex = r"^([<>]?)(\d+\.?\d*)$"
    insufficient_regex = r"^(I[\.]?S[\.]?)$"
    not_analyzed_regex = r"^(N[\.]?[AD]{1}[\.]?)$"

    missing_values = [""] + [" ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "NULL", "NaN", "None", "n/a", "nan", "null"]
    normalize_values = {
        ',': ".",
        "-": "<",
        "<.": "<0.",
    }

    # Pipes
    def handle_missing(series, extra_missing_values=[]):
        return (
            series.str.replace(spaces_regex, "", regex=True)
                .replace(missing_values + extra_missing_values, None) 
        )

    def handle_normalized(series, replaces={}):
        for key, value in replaces.items():
            series = series.str.replace(key, value)
        return series
        
    # Primeiras limpezas
    # index_names = [col.name for col in (assay_sample_column, assay_analyte_column)]
    # value_name = f"{assay_value_column.name}_{assay_qualif_column.name}"

    # Ler dados bronze
    df = pd.read_parquet(dataset) 

    # Tratamento de dados de amostras
    indexColumns = etl.destination.assayTable.indexColumns
    valueColumns = etl.destination.assayTable.valueColumn

    assert len(indexColumns) == 3, "destination.assayTable.indexColumns precisa ter comprimento 3, com as colunas de amostra, analito e unidade, nesta ordem"
    assert len(valueColumns) == 2, "destination.assayTable.valueColumns precisa ter comprimento 2, com as colunas de qualificador e valor, nesta ordem"

    index_columns_axis = indexColumns[:2]
    valueColumns_join = "_".join(valueColumns)

    # Tratamento de dados de amostras
    assay_df = (
        df.filter(assay_cols)
            .apply(lambda col: col.replace("", None))
            .rename(columns=lambda col: slugify(col, separator="_"))
            # Traz o objectid para o index do dataframe
            # .set_index([col.name for col in assay_columns_fixed], append=True)
            # De-pivot
            .stack()
            # Ajusta nomes
            .rename_axis(index_columns_axis)
            .rename(valueColumns_join)
            # handle missing data on values
            .pipe(handle_missing)
            .dropna()
            # normalize values  qualificators
            .pipe(handle_normalized, normalize_values)
    )

    # Tratamento de index
    _analyte_name, _unit_name = indexColumns[1:]

    assay_df = (
        assay_df
            .to_frame()
            .join(
                pd.DataFrame(
                    assay_df.index.get_level_values(_analyte_name).str.rsplit("_", n=1, expand=True).to_list(), 
                        index=assay_df.index, 
                        columns=[_analyte_name, _unit_name]
                    )
            )
            .reset_index(level=_analyte_name, drop=True)
            .set_index([_analyte_name, _unit_name], append=True)
    )

    # Index tem que ser único
    assert assay_df.index.is_unique

    # Decomposição de valores para valor, qualificador
    # Valores precisam atender a padrão de valores
    values_match = assay_df.squeeze().astype(str).str.match(values_match_regex)
    insufficient_match = assay_df.squeeze().astype(str).str.match(insufficient_regex)
    not_analyzed_match = assay_df.squeeze().astype(str).str.match(not_analyzed_regex)

    invalid_values = assay_df[~(values_match | insufficient_match | not_analyzed_match)]

    # try:
    assert invalid_values.size == 0, f"Alguns valores <{invalid_values.shape[0]}> não coincidiram com a expressão regular: \n{invalid_values.head()}"
        
    # except AssertionError as e:
    #     logging.warn(e)
    #     assay_error_oids = invalid_values.index.get_level_values(0).drop_duplicates().tolist()

    #     # Write errors
    #     out_assay_error_file = create_filename(src_schema, src_table_name, "csv", table_item, suffix="assay_errors")
    #     df[df.index.isin(assay_error_oids)].to_csv(out_assay_error_file, index=True)
    #     logging.warning(f"Amostras com problemas de validação de valores estão salvas no arquivo {out_assay_error_file}")

    # Normalizar valores com o padrão SGB/CPRM
    _pivot_col_names = dict(enumerate(valueColumns_join.split("_")))
    _qualif_col, _value_col = list(_pivot_col_names.values())

    assay_df = (
        assay_df.join(
            pd.concat([
                assay_df[valueColumns_join].str.extract(values_match_regex).dropna(),
                assay_df[valueColumns_join].str.extract(insufficient_regex).dropna().replace(insufficient_regex, "I", regex=True),
                assay_df[valueColumns_join].str.extract(not_analyzed_regex).dropna().replace(not_analyzed_regex, "N", regex=True)
            ])
                .rename(columns=_pivot_col_names)
        )
        .assign(
            **{
                _value_col: lambda df: pd.to_numeric(df[_value_col].fillna(0), errors="raise"),
                _qualif_col: lambda df: df[_qualif_col].pipe(handle_missing).astype("category")
            }
        )
        .drop(
            valueColumns_join,
            axis="columns"
        )    
    )

    # Gravar em Parquet
    return export_parquet(
        assay_df, 
        f"{etl.name}/silver", 
        f"{etl.destination.schema}_{etl.destination.assayTable.name}.parquet"
    )