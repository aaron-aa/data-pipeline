{
    "owner": "ss",
    "type": "dimension",  # ?
    "granularity": "daily",  # 2017-11-24/-1
    "volume": "mb",  # data size per granularity
    "retention": "",
    "schema": {
        "namespace": "feature.v2.apple-store.story",
        "type": "record",
        "name": "Story",
        "aliases": ["Story"]
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "device_code", "default": 'iphone', "type": [
                {"type": "enum", "name": "device", "symbols": ["iphone", "ipad"]}, "null"]},
            {"name": "market_code", "default": 'apple-store',
                "type": [{"type": "enum", "name": "market", "symbols": ["apple-store"]}, "null"]},
            {"name": "country_code", "type": "string"},
            {"name": "date", "type": "string"},
            {"name": "url", "type": "string"},
            {"name": "position", "type": "int"},
            {"name": "creative_urls", "type": ["null", {"type": "array", "items": "string"}]},
            {"name": "app_ids", "type": ["null", {"type": "array", "items": "long"}]},
            {"name": "label", "type": ["null", "string"]},
            {"name": "head", "type": ["null", "string"]},
            {"name": "description", "type": ["null", "string"]},
            {"name": "content", "type": ["null", "string"]},
            {"name": "display_style", "type": ["null", {"type": "map", "values": "string"}]},
            {"name": "raw_data", "type": "string"}
        ]
    }
}
