from airflow.providers.postgres.hooks.postgres import PostgresHook
from sgb.common.sql.hooks import GeoPandasMixin


class GeoPandasMsSqlHook(PostgresHook, GeoPandasMixin):
    pass
