from airflow.providers.oracle.hooks.oracle import OracleHook
from sgb.common.sql.hooks import GeoPandasMixin


class GeoPandasOracleHook(GeoPandasMixin, OracleHook):
    pass
