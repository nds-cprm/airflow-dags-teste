import yaml

from dataclasses import dataclass, field
from typing import Tuple, Optional, Any

@dataclass(kw_only=True)
class Coordinates:
    xColumn: str = "longitude"
    yColumn: str = "latitude"
    srid: int = 4674

@dataclass(kw_only=True)
class TimestampColumn:
    name: str = "data_de_analise"
    format: str = "%d/%m/%Y"

@dataclass(kw_only=True)
class SourceETL:
    schema: str
    table: str
    geometryColumn: Coordinates
    primaryKeyColumn: str = "objectid"
    connectionName: Optional[str]
    timestampColumn: Optional[Tuple[TimestampColumn]]

@dataclass(kw_only=True)
class SurveyTable:
    name: str
    primaryKeyColumn: str = "fid"
    geometryColumn: str = "geometry"

@dataclass(kw_only=True)
class AssayTable:
    name: str
    indexColumns: Tuple[str] = (
        "amostra",
        "analito"
    )
    valueColumn: str = "valor"
    excludedColumns: Tuple[str] = (
        "globalid",
        "lote",
        "ra",
        "metodo",
        "created_user",
        "created_date",
        "last_edited_user",
        "last_edited_date"
    )

@dataclass(kw_only=True)
class DestinationETL:
    connectionName: Optional[str]
    schema: str
    surveyTable: SurveyTable
    assayTable: AssayTable
    matViews: Optional[Tuple[str]]

@dataclass(kw_only=True)
class GeoquimicaETLConfig:
    """
    _summary_
    """
    name: str
    schedule: Optional[str] = None
    description: Optional[str] = None
    tags: Tuple[str] = ()
    dagArgs: dict[str:Any] = field(default_factory=dict)
    source: SourceETL
    destination: DestinationETL

    @classmethod
    def from_yaml(cls, config_file):
        with open(config_file, 'r') as file:
            data = yaml.safe_load(file)
        
        etl = data.pop("etl")

        # Source ETL
        source = etl.pop("source")
        coordinates = Coordinates(**source.pop("geometryColumn"))
        timestamp_col = [TimestampColumn(**i) for i in source.pop("timestampColumn", [])]
        source = SourceETL(geometryColumn=coordinates, timestampColumn=timestamp_col, **source)

        # Destination ETL
        destination = etl.pop("destination")
        survey_table = SurveyTable(**destination.pop("surveyTable"))
        assay_table = AssayTable(**destination.pop("assayTable"))
        destination = DestinationETL(surveyTable=survey_table, assayTable=assay_table, **destination)

        return cls(source=source, destination=destination, **data)


    
    
