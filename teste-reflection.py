import logging
import geopandas as gpd
import oracledb
import sys

from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.exc import ProgrammingError

from utils.sde import STGeometry, SDETable


logging.basicConfig(level=logging.INFO)

sys.modules["cx_Oracle"] = oracledb
oracledb.version = "8.3.0"
oracledb.init_oracle_client()


# bases para migrar
sde_layers = [
    "recmin.rm_layer",
    "aflora.af_layer",
    "geocronologia.gr_layer",
    "petrografia.pet_layer",
    "paleo.pl_layer",    
    "litoestratigrafia.ue_layer",
]

module_dir = Path(__file__).parent


for sde_layer in sde_layers:
    logging.info("Inciando conversão da tabela <%s> no Oracle...", sde_layer)
    start_date = datetime.now()

    # Banco de dados de origem
    src_dsn = module_dir / "notebooks/oracle-dsn.txt"
    src_schema, src_table_name = sde_layer.split(".")

    # Banco de dados de destino
    dst_dsn = module_dir / "notebooks/postgres-dsn.txt"
    dst_schema, dst_table_name = "_sgb", f"{src_schema}_{src_table_name}"
    dst_pk_name, dst_geom_name = "fid", "geometry"

    # Que comecem os jogos
    with open(src_dsn) as f:
        logging.info("Lendo DSN do banco de dados Oracle...")
        src_engine = create_engine(f.read())

    try:
        logging.info("Fazendo a reflexão da tabela no Oracle...")
        src_metadata = MetaData(schema=src_schema)
        a = datetime.now()
        src_table = SDETable(src_table_name, src_metadata, autoload_with=src_engine)
        logging.info("Tabela refletida com sucesso")

    except Exception as e:
        logging.error("Não foi possível refletir a tabela <%s.%s>: %s", src_schema, src_table_name, str(e))
        print(str(datetime.now() - a))
        raise e

    # Metadados da tabela
    src_pk_col = src_table.sde.rowid_name
    src_geom_col = src_table.geometry_columns[0] # Vai pegar a primeira

    # Oracle para geopandas
    with src_engine.connect() as conn:
        logging.info("Carregando dados do Oracle em GeoDataFrame...")
        geodata = (
            gpd.read_postgis(
                str(src_table.select()), 
                conn,
                geom_col = src_geom_col.name,
                crs = src_geom_col.type.srid
            )
            .rename_geometry(dst_geom_name)
            .rename(columns={src_pk_col: dst_pk_name})
            .set_index(dst_pk_name)
        )

        logging.info("Dados carregados em GeoDataFrame")

    assert geodata.index.is_unique, "O dataset contém FIDs duplicados"

    geodata.info()

    # Gravar em parquet 
    try:
        logging.info("Exportando para GeoParquet...")
        geodata.to_parquet( module_dir / f"data/geobank_{src_schema}_{src_table_name}.parquet", index=True)
        logging.info("Exportação concluída")
    except Exception as e:
        logging.warning("Problema na exportação do GeoParquet: %s", str(e))

    # Gravar em GeoPackage
    try:
        logging.info("Exportando para GeoPackage...")
        geodata.convert_dtypes().to_file(module_dir / "data/geobank_oracle11g.gpkg", index=True, layer=src_table_name)
        logging.info("Exportação concluída")
    except Exception as e:
        logging.warning("Problema na exportação do GeoPackage: %s", str(e))

    # Copiar a estrutura para o PostgreSQL
    with open(dst_dsn) as f:
        logging.info("Lendo DSN do banco de dados PostgreSQL...")
        dst_engine = create_engine(f.read(), connect_args={"application_name": "ETL GeoBank"})

    dst_metadata = MetaData()

    logging.info("Copiando objeto Table do Oracle para o PostgreSQL...")
    dst_table = src_table.to_metadata(
        dst_metadata, 
        schema=dst_schema, 
        name=dst_table_name
    )
    logging.info("Cópia concluída")

    # Manipulação de indices
    dst_table.indexes.clear()

    # Cria a tabela
    logging.info("Criando estrutura no PostgreSQL...")
    dst_table.create(bind=dst_engine, checkfirst=True)
    logging.info("Estrutura criada com sucesso")

    # Cria PK e renomeia as colunas
    with dst_engine.connect() as conn:
        logging.info("Atualizando os nomes de colunas de chave primária e geometria, além de criar PK Constraint...")
        trans = conn.begin()

        for sql in [
            f"ALTER TABLE {dst_schema}.{dst_table_name} RENAME COLUMN {src_pk_col} TO {dst_pk_name};",
            f"ALTER TABLE {dst_schema}.{dst_table_name} RENAME COLUMN {src_geom_col.name} TO {dst_geom_name};",
            f"ALTER TABLE {dst_schema}.{dst_table_name} ADD CONSTRAINT {dst_table_name}_pk PRIMARY KEY ({dst_pk_name});"
        ]:
            try:
                conn.execute(text(sql))

            except ProgrammingError:
                logging.warning("Execução de instrução SQL ignorada: %s" % sql)
                trans.rollback()
        
        logging.info("Alteração concluída...")

        time_taken = str(datetime.now() - start_date)

    logging.info("Tabela <%s> concluída em %s.", sde_layer, time_taken)