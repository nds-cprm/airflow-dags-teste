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
                     "id": {
                        "type": "integer"
                    },
                    "sigla": {
                        "type": "keyword",
                    }, 
                    "hierarquia": {
                        "type": "keyword",
                    }, 
                    "nome": {
                        "type": "keyword",
                    }, 
                    "ambiente_tectonico": {
                        "type": "keyword",
                    },
                    "sub_ambiente_tectonico": {
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
                        "type": "keyword",
                    }, 
                    "range": {
                        "type": "keyword",
                    },
                    "idade": {
                        "min": {
                            "type": "float",
                        },
                        "max": {
                            "type": "float",
                        }
                    },
                    "eon":{
                        "min": {
                            "type": "keyword",
                        },
                        "max": {
                            "type": "keyword",
                        }
                    },
                    "era": {
                        "min": {
                            "type": "keyword",
                        },
                        "max": {
                            "type": "keyword",
                        }
                    },
                    "sistema": {
                        "min": {
                            "type": "keyword",
                        },
                        "max": {
                            "type": "keyword",
                        }
                    },
                    "epoca": {
                        "min": {
                            "type": "keyword",
                        },
                        "max": {
                            "type": "keyword",
                        }
                    },
                    "siglas_historicas": {
                        "type": "keyword",
                    }, 
                    "grupo": {
                        "type": "keyword",
                    }
                }
            }
        }
    }
}
