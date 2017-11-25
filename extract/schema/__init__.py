import boto3
import cStringIO
import json
import pprint

from avro import schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from utils.s3 import put


def read(schema_definition, s3_path, spark=None):
    if not spark:
        output = StringIO.StringIO()
        latest_file_object = s3_client.Object('bucket_name', 'latest_file')
        latest_file_object.download_fileobj(output)

        reader = DataFileReader(output, DatumReader())
        for r in reader:
            print r

            schema = avro.schema.parse(schema_["schema"])
            reader = DataFileReader(s3_path, DatumReader((readers_schema=schema)))
            for record in reader:
                yield record
            reader.close()
    else:
        return spark.read.format("com.databricks.spark.avro").load(s3_path)


def write(schema_, data):
    """
    """
    try
        _is_pyspark_dataframe(data)
        if _is_valiad_schema(schema_, data):
            data_container.write.mode('overwrite').format(
                "com.databricks.spark.avro").save("/Users/aaron/Downloads/episodes.avro")
        else:
            raise Exception("dataframe schema is not same as the defined")
    except TypeError:
        io = cStringIO.StringIO()
        writer = DataFileWriter(io, DatumWriter(), schema_["schema"], codec="deflate")
        for r in data:
            writer.append(r)
        writer.close()
        retry(put, ("aaron-s3-poc", "datapipeline/extract/test", io.get_value()))


def _is_pyspark_dataframe(df):
    return getattr(data_container, 'toDF') and getattr(data_container, 'repartition')


def _is_valiad_schema(schema_, df):
    # [('title', 'string'), ('air_date', 'string'), ('doctor', 'int')]
    dtypes = df.dtypes
    return sorted(schema_["schema"]['fields'].keys()) == sorted(map(lambda x: x[0], dtypes))


def retry(func, params, retry_count=10):
    """
    Try retry_count times for func if something wrong happened with 1 second interval
    Args:
        func: function you want execute
        params: tuple, params pass to func
    Returns:
        result return by func
    """

    tries = 0

    while tries < retry_count:
        try:
            return func(*params)
        except Exception as e:
            print 'failed to {func}, tried {n} times because of {e}'.format(func=func.__name__, n=(tries + 1), e=e)
            tries += 1
            time.sleep(1)

    if tries >= retry_count:
        raise Exception("failed to retry {func} {n} times".format(func=func.__name__, n=tries))
