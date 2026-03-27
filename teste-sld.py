import oracledb
import sys
import pandas as pd

sys.modules["cx_Oracle"] = oracledb
oracledb.version = "8.3.0"
oracledb.init_oracle_client()

from pathlib import Path
from sqlalchemy import create_engine
from sld import StyledLayerDescriptor, PolygonSymbolizer


dsn = Path(__file__).parent / "notebooks/oracle-dsn.txt"


with open(dsn) as f:
    engine = create_engine(f.read())

with engine.connect() as conn:
    colors = pd.read_sql_query(
        "SELECT id, sigla, r, g, b FROM litoestratigrafia.ue_unidade_estratigrafica", 
        conn, 
        index_col="id"
    ).dropna(how="any", subset=["r", "g", "b"])

colors.info()

# SLD
_sld = StyledLayerDescriptor()
nl = _sld.create_namedlayer("litoestratigrafia")
ustyle = nl.create_userstyle()
fts = ustyle.create_featuretypestyle()

for row in colors.itertuples():
    rgb_color = (int(row.r), int(row.g), int(row.b))

    rule = fts.create_rule('Unidade {}'.format(row.sigla), PolygonSymbolizer)
    rule.PolygonSymbolizer.Fill.CssParameters[0].Value = "#{:02X}{:02X}{:02X}".format(*rgb_color)
    rule.PolygonSymbolizer.Stroke.CssParameters[0].Value = '#FFFFFF' # White stroke
    rule.PolygonSymbolizer.Stroke.CssParameters[1].Value = '0'
    rule.create_filter('sigla', '==', row.sigla)

# _sld.validate()

with open(Path(__file__).parent / "sgb.sld", "wb") as f:
    f.write(_sld.as_sld(pretty_print=True))