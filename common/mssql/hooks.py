from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from sgb.common.sql.hooks import GeoPandasMixin


class GeoPandasMsSqlHook(MsSqlHook, GeoPandasMixin):
    pass