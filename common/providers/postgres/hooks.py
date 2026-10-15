from airflow.providers.postgres.hooks.postgres import PostgresHook
from sgb.common.providers.sql.hooks import GeoPandasMixin


class GeoPandasMsSqlHook(PostgresHook, GeoPandasMixin):
    pass
