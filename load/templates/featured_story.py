INTERFACE = {
    "index": "int-reviews-$platform-$month",
    "doc_type": "$platform",
    "host": "INT_REVIEWS_ES_HOST",
    "ssl": false,
    "basic_auth": true,
    "username": "INT_REVIEWS_AMR_ES_USER",
    "password": "INT_REVIEWS_AMR_ES_PASSWORD",
    "schema": {
        "template": "int-ss-featured-story-*",
        "settings": {
            "index": {
                "codec": "best_compression",
                "search": {
                    "slowlog": {
                        "threshold": {
                            "fetch": {
                                "warn": "5s",
                                "info": "2s"
                            },
                            "query": {
                                "warn": "5s",
                                "info": "2s"
                            }
                        }
                    }
                },
                "refresh_interval": "1s",
                "number_of_shards": "3",
                "translog": {
                    "flush_threshold_size": "512mb",
                    "sync_interval": "30s",
                    "durability": "async"
                },
                "merge": {
                    "scheduler": {
                        "max_thread_count": "8"
                    }
                },
                "max_result_window": "1000000",
                "mapper": {
                    "dynamic": "false"
                },
                "requests": {
                    "cache": {
                        "enable": "true"
                    }
                },
                "analysis": {
                    "analyzer": {
                        "whitespace": {
                            "type": "whitespace"
                        }
                    }
                },
                "number_of_replicas": "0"
            }
        },
        "mappings": {
            "ios": {
                "_routing": {
                    "required": true
                },
                "dynamic": "strict",
                "_source": {
                    "enabled": true
                },
                "_all": {
                    "enabled": false
                },
                "properties": {
                    "featured_story_id": {
                        "index": true,
                        "store": false,
                        "type": "long",
                        "doc_values": true
                    },
                    "date": {
                        "index": true,
                        "store": false,
                        "type": "date",
                        "format": "basic-date",
                        "doc_values": true
                    },
                    "utime": {
                        "index": false,
                        "store": false,
                        "type": "date",
                        "doc_values": true
                    },
                    "market_code": {
                        "index": true,
                        "store": false,
                        "type": "keyword",
                        "doc_values": true
                    },
                    "device_code": {
                        "index": true,
                        "store": false,
                        "type": "keyword",
                        "doc_values": true
                    },
                    "display_style": {
                        "index": false,
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "rank": {
                        "index": false,
                        "store": false,
                        "type": "keyword",
                        "doc_values": true
                    },
                    "country_code": {
                        "index": true,
                        "store": false,
                        "type": "keyword",
                        "doc_values": true
                    },
                    "language": {
                        "index": true,
                        "store": false,
                        "type": "keyword",
                        "doc_values": true
                    },
                    "label": {
                        "index": false,
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "label_t": {
                        "analyzer": "whitespace",
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "head": {
                        "index": false,
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "head_t": {
                        "analyzer": "whitespace",
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "description": {
                        "index": false,
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "description_t": {
                        "analyzer": "whitespace",
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "content": {
                        "index": "false",
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "content_t": {
                        "analyzer": "whitespace",
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "product_ids": {
                        "index": true,
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    },
                    "creative_urls": {
                        "index": false,
                        "store": false,
                        "type": "text",
                        "doc_values": false
                    }
                }
            }
        },
        "aliases": {
            "{index}-alias": {}
        }
    }
}
