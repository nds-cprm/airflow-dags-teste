# Adaptado de https://github.com/geoalchemy/geoalchemy2/blob/main/geoalchemy2/admin/__init__.py
import logging
import warnings

from sqlalchemy import text, event, Table

from .types import STGeometry


log = logging.getLogger("geosgb.utils.admin")


def setup_ddl_event_listeners():
    @event.listens_for(Table, "column_reflect")
    def column_reflect(inspector, table, column_info):
        table_name = table.fullname
        col_name = column_info["name"]
        col_type = column_info["type"]

        # Específico para ST_Geometry
        if type(col_type) is STGeometry: 
            conn = inspector.bind

            # primeiro verifica se tem registros na tabela.
            log.info("Retrieving if it has rows...")
            num_rows = conn.execute(
                text(f"SELECT count(*) FROM {table_name}")
            ).scalar()
            log.info("Retrieve count of rows OK.")

            if num_rows == 0:
                warnings.warn(f"A coluna {col_name} da tabela {table_name} não contém registros. Não será possível obter as propriedades do ST_Geometry a partir dos dados")
                return

            # Obter as propriedades a partir dos dados
            # SRID
            log.info("Retrieving SRID...")
            column_info["type"].srid = conn.execute(
                text(f"SELECT DISTINCT sde.st_srid({col_name}) FROM {table_name}")
            ).scalar()
            log.info("Retrieve SRID OK.")

            # Geometry_type (POINT, LINESTRING, POLYGON, no MULTI, No dimensions)
            log.info("Retrieving geometry type...")
            _types = [row[0] for row in conn.execute(
                text(f"SELECT DISTINCT replace(replace(upper(sde.st_geometrytype({col_name})), 'ST_', ''), 'MULTI', '') FROM {table_name}")
            ).fetchall()]

            geometry_type = _types[0].upper() if len(_types) == 1 else 'GEOMETRYCOLLECTION'                

            # Simple or multipart
            log.info("Checking if it is single or multi part...")
            is_multi = bool(conn.execute(
                text(f"SELECT max(sde.st_numgeometries({col_name})) FROM {table_name}")
            ).scalar() > 1)

            # Define verbose geometry_type and correct dimensions
            if geometry_type != "GEOMETRYCOLLECTION" and is_multi:
                geometry_type = "MULTI" + geometry_type

            log.info("Retrieve geometry type OK.")
            
            # Analyze dimensions
            dimension = 2 

            # check 3d
            log.info("Checking if it is 3D...")
            is_3d = any([row[0] for row in conn.execute(
                text(f"SELECT DISTINCT sde.st_is3d({col_name}) FROM {table_name}")
            ).fetchall()])

            if is_3d:
                dimension += 1
                geometry_type = geometry_type + "Z"
                
            log.info("Check if it is 3D OK...")

            # check measured
            log.info("Checking if it is measured...")
            is_measured = any([row[0] for row in conn.execute(
                text(f"SELECT DISTINCT sde.st_ismeasured({col_name}) FROM {table_name}")
            ).fetchall()])

            if is_measured:
                dimension += 1
                geometry_type = geometry_type + "M"
            
            log.info("Check it is measured OK.")
            
            # Set values
            column_info["type"].geometry_type = geometry_type
            column_info["type"].dimension = dimension

            # has_spatial_index (Ainda não faz índice espacial)
            column_info["type"]._spatial_index_reflected = False
        else:
            # Nivela para tipo genérico do SQLAlchemy
            column_info["type"] = col_type.as_generic()
        

__all__ = [
    "setup_ddl_event_listeners",
]

def __dir__():
    return __all__
