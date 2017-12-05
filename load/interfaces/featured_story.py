from utils.avro_ import create

INTERFACE = {
    "schema": create({
        "namespace": "es.schema.v1",
        "type": "record",
        "name": "ESSchema",
        "fields": [
            {"name": "index_pattern", "type": "string"},
            {"name": "doc_type", "type": ["int", "string"]},
            {"name": "host", "type": "string"},
            {"name": "support_basic_auth", "type": "boolean"},
            {"name": "basic_auth_user", "type": ["null", "string"]},
            {"name": "basic_auth_password", "type": ["null", "string"]},
            {"name": "primary_fields", "type": {"type": "array", "items": "string"}},
            {"name": "fields", "type": {"type": "map", "values": "string"}}
        ]
    })
}
