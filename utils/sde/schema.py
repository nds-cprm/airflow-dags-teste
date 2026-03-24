import ast
import logging

from dataclasses import dataclass
from sqlalchemy import Table, text
from utils.sde import STGeometry


log = logging.getLogger("geosgb.utils.schema")


@dataclass
class SDEProperties:
    version: tuple
    rowid_name: str
    is_simple: bool 
    is_versioned: bool
    is_replicated: bool 

    def get_version(self):
        return f"ArcSDE: %s.%s.%s" & self.version
    

class SDETable(Table):
    _sde = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        engine = kwargs.get("autoload_with")        

        if engine:
            with engine.connect() as conn:
                # SDE version
                _table = 'sde.sde_version'

                # TODO: Verificar difernças com PostgreSQL
                if conn.dialect.name == 'oracle':
                    _table = 'sde.version'
                
                log.info("Retrieving ArcGIS verison...")
                version = conn.execute(
                    text(f"SELECT major, minor, bugfix FROM {_table}")                    
                ).fetchone()
                log.info("Retrieve ArcGIS OK.")

                # table args
                table_args = {"table": self.name, "schema": self.schema}

                # Identify rowid
                log.info("Retrieving RowID name...")
                rowid_name = conn.execute(
                    text("SELECT sde.gdb_util.rowid_name(:schema, :table) FROM DUAL"),
                    table_args
                ).scalar()
                log.info("Retrieving RowID OK.")
                
                rowid_column = tuple(filter(lambda col: col.name.upper() == rowid_name.upper(), self.columns))

                if rowid_column:
                    rowid_name = rowid_column[0].name

                # Is simple
                log.info("Retrieving if FeatureClass/Table is simple...")
                is_simple = conn.execute(
                    text("SELECT sde.gdb_util.is_simple(:schema, :table) FROM DUAL"),
                    table_args
                ).scalar()
                log.info("Retrieved if is simple OK.")

                is_simple = ast.literal_eval(is_simple.strip().title())

                # Is versioned
                log.info("Retrieving if FeatureClass/Table is versioned...")
                is_versioned = conn.execute(
                    text("SELECT sde.gdb_util.is_versioned(:schema, :table) FROM DUAL"),
                    table_args
                ).scalar()
                log.info("Retrieved if is versioned OK.")

                is_versioned = ast.literal_eval(is_versioned.strip().title())

                # Is replicated
                log.info("Retrieving if FeatureClass/Table is replicated...")
                is_replicated = conn.execute(
                    text("SELECT sde.gdb_util.is_replicated(:schema, :table) FROM DUAL"),
                    table_args
                ).scalar()
                log.info("Retrieve if is replicated OK.")

                is_replicated = ast.literal_eval(is_replicated.strip().title())

            self._sde = SDEProperties(version, rowid_name, is_simple, is_versioned, is_replicated)

    @property
    def sde(self):
        return self._sde
    
    @property
    def geometry_columns(self):
        return tuple(filter(lambda col: type(col.type) is STGeometry, self.columns))
        