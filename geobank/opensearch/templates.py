ID_COLUMN = "fid"
GEOMETRY_COLUMN = "geometry"

# Configuração base de mapeamento e settings para o OpenSearch 3.7
INDEX_BODY_TEMPLATE = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "refresh_interval": "-1",  #  Se colocar o valor para -1, forçar a reindexação: POST geo-dados-v1/_refresh
            "translog": {
                "durability": "async",
                "sync_interval": "5s"
            }
        }
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "type": {
                "type": "keyword"
            },
            GEOMETRY_COLUMN: {
                "type": "geo_shape",
                "ignore_malformed": False,
                "ignore_z_value": True
            },
            "properties": {
                "properties": {
                    ID_COLUMN: {
                        "type": "integer"
                    },
                    "id_uni_estratigrafica": {
                        "type": "integer",
                    },
                    "sigla": {
                        "type": "keyword",
                    }, 
                    "hierarquia": {
                        "type": "keyword",
                    }, 
                    "nome": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, 
                    "amb_tecto": {
                        "type": "keyword",
                    },
                    "sub_amb_tectonico": {
                        "type": "keyword",
                    },
                    "sigla_pai": {
                        "type": "keyword",
                    },
                    "nome_pai": {
                        "type": "keyword",
                    },
                    "legenda": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, 
                    "escala": {
                        "type": "keyword",
                    }, 
                    "mapa": {
                        "type": "keyword",
                    }, 
                    "litotipos": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, 
                    "range": {
                        "type": "text",
                        "analyzer": "brazilian"
                    },
                    "idade_min": {
                        "type": "float",
                    }, # Real (0.0)
                    "idade_max": {
                        "type": "float",
                    }, 
                    "eon_min": {
                        "type": "keyword",
                    },
                    "eon_max": {
                        "type": "keyword",
                    },
                    "era_min": {
                        "type": "keyword",
                    },
                    "era_max": {
                        "type": "keyword",
                    },
                    "sistema_min": {
                        "type": "keyword",
                    }, 
                    "sistema_max": {
                        "type": "keyword",
                    },
                    "epoca_min": {
                        "type": "keyword",
                    }, 
                    "epoca_max": {
                        "type": "keyword",
                    }, 
                    "siglas_historicas": {
                        "type": "text",
                        "analyzer": "brazilian"
                    }, 
                    "grupo": {
                        "type": "keyword",
                    }
                }
            }
        }
    }
}
