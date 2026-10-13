import logging


log = logging.getLogger("airflow.task")


def get_postgres_table_colums(hook, schema, table):
    get_cols_sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position;"
    survey_column_names = tuple([row[0] for row in tuple(hook.get_records(get_cols_sql, (schema, table)))])
    log.info("Column names for %s.%s: %s", schema, table, ", ".join(survey_column_names))
    return survey_column_names