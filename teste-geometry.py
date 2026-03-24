import geopandas as gpd

from pathlib import Path
from sqlalchemy import create_engine


engine = create_engine("postgresql://p3m_layers:Z484dm!zQ!Yrfc57AcWo@lrnpdbms05.sgb.local:5432/p3m_layers")


datadir = Path(__file__).parent / 'data'

for item in datadir.iterdir():
    if item.is_file() and item.name.endswith(".parquet") and "aflora" in item.name:
        table_name = item.name.replace("geobank_", "").replace(".parquet", "")
        print(f"File: {item.name} to table {table_name}")
        
        data = gpd.read_parquet(datadir / item)
        str_cols = data.select_dtypes(include="object").columns

        data[str_cols] = data[str_cols].apply(lambda x: x.str.replace('\x00', '', regex=False))

        data.to_postgis(table_name, engine, if_exists="replace", schema="geosgb", index=True, index_label="fid", chunksize=5000)

        print(data.geometry.is_valid.value_counts())



# Aflora tem Null byte no seu conteúdo
#psycopg2.errors.CharacterNotInRepertoire: invalid byte sequence for encoding "UTF8": 0x00 CONTEXT:  COPY aflora_af_layer, line 3187
