import json

from avro import schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def create(dict_):
    return schema.parse(json.dumps(dict_))


def beautify_schema(schema_):
    return json.dumps(schema_.to_json(schema.Names()), indent=2)


def write(schema_, file_, data, codec="deflate"):
    writer = DataFileWriter(file_, DatumWriter(), schema_, codec=codec)
    for r in data:
        writer.append(r)
    writer.close()
