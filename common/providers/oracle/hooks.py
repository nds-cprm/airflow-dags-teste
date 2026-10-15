from airflow.providers.oracle.hooks.oracle import OracleHook
from sgb.common.providers.sql.hooks import GeoPandasMixin


class GeoPandasOracleHook(GeoPandasMixin, OracleHook):
    pass
