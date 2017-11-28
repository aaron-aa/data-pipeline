# https://github.com/appannie/aa/blob/master/estimation/vans/README.md
# must be no default value

INTERFACE = {
    "alias": "story",
    "partition_fields": ["year", "month", "day", "market_code", "device_code", "country_code"],
    "volume": "mb",
    "retention": "-1",
    "definition": {
        "namespace": "ss.story.v1",
        "type": "record",
        "name": "Story",
        "aliases": ["story"],
        "fields": [
            {"name": "data_type", "type": {"type": "enum", "name": "dtype", "symbols": ["dimension"]}},
            {"name": "data_granularity", "type": {"type": "enum", "name": "dgranularity", "symbols": ["daily"]}},
            {"name": "device_code", "type": [
                {"type": "enum", "name": "device", "symbols": ["iphone", "ipad"]}, "null"]},
            {"name": "market_code", "type": [{"type": "enum", "name": "market", "symbols": ["apple-store"]}, "null"]},
            {"name": "country_code", "type": "string"},
            {"name": "year", "type": "int"},
            {"name": "month", "type": "int"},
            {"name": "day", "type": "int"},
            {"name": "hour", "type": ["null"]},
            {"name": "story_id", "type": "long"},
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
