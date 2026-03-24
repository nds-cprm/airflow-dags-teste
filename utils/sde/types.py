from sqlalchemy.sql import func
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.dialects.postgresql.base import PGDialect
from geoalchemy2.types import Geometry


COLUMN_TYPENAME = 'ST_GEOMETRY'


class STGeometry(Geometry):
    name = COLUMN_TYPENAME
    from_text = "sde.st_geomfromtext"
    as_binary = "sde.st_asbinary"

    # SDE specific properties
    rowid_name = None
    is_simple = None
    is_archive = None
    is_versioned = None 
    is_replicated = None

    def __init__(
            self, 
            geometry_type = "GEOMETRY", 
            srid = -1, 
            dimension = None, 
            spatial_index = True, 
            nullable = True, 
            _spatial_index_reflected=None
        ):
        super().__init__(
            geometry_type=geometry_type, 
            srid=srid, 
            dimension=dimension, 
            spatial_index=spatial_index, 
            use_N_D_index=False,
            use_typmod=False,
            nullable=nullable, 
            _spatial_index_reflected=_spatial_index_reflected
        )

    def get_col_spec(self, **kw):
        return COLUMN_TYPENAME

    def column_expression(self, col):
        """
        No SELECT, converte o objeto ESRI para WKB usando a função do SDE.
        """
        return func.sde.st_asbinary(col)

    def bind_expression(self, bindvalue):
        return func.sde.st_geomfromtext(bindvalue)

# Register Geometry, Geography and Raster to SQLAlchemy's reflection subsystems.
OracleDialect.ischema_names[COLUMN_TYPENAME] = STGeometry
PGDialect.ischema_names[COLUMN_TYPENAME] = STGeometry


__all__ = [
    "STGeometry",
]

def __dir__():
    return __all__
