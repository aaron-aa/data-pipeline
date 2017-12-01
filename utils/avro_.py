import json

from avro import schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def write(schema_, file_, data, codec="deflate"):
    avro_schema = schema.parse(json.dumps(schema_))
    writer = DataFileWriter(file_, DatumWriter(), avro_schema, codec=codec)
    for r in data:
        writer.append(r)
    writer.close()
