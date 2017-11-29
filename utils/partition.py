import datetime
import hashlib
import json

from pyspark.sql.types import NullType, LongType, IntegerType, StringType
from pyspark.sql.functions import lit, col

#"granularity": "daily",  # daily/2017-11-24/None/, hourly/2017-11-24/23/, yearly/2017-01-01/None/, monthly/2017-01-31/None,

PARTITION_FIELDS = [
    ("data_type", StringType),
    ("primary_dimension", StringType),
    ("data_granularity", StringType),
    ("date", StringType),
    ("market_code", StringType),
    ("device_code", StringType),
    ("country_code", StringType),
    ("data_name", StringType)
]


def signature(data):
    if data:
        md5 = hashlib.md5(json.dumps(data)).hexdigest()
        return "dp{}".format(md5)
    else:
        return ""


def data_partition_name(fields):
    return "-".join(map(lambda (x, y): "{}={}".format(x, y), fields))


def identifier():
    return datetime.datetime.today().strftime('%Y%m%d%H%M%S')


def _namespace(ns):
    return ns.replace(".", "/")


def s3key_of_rawfile(namespace, manipulation, identifier, partition_fields,
                     signature=None, format='avro'):
    """
    ss/story/v1/insert/20171127171302-dpa9d9b034887206139fba4dd18d925c6d-2017-11-31-apple-store-iphone-JP.avro
    ss/story/v1/insert/20171127171302-dpa9d9b034887206139fba4dd18d925c6d-2017-11-31-apple-store-iphone-US.avro
    """
    key = "raw/{namespace}/{identifier}" + \
        "{signature_sep}{signature}{signature_sep}{data_partition_name}.{format}"
    return key.format(namespace=_namespace(namespace),
                      identifier=identifier,
                      format=format,
                      signature="signature={}".format(signature),
                      signature_sep="-" if signature else "",
                      data_partition_name=data_partition_name(partition_fields))


def s3key_of_rawfile_with_mildcard(namespace, manipulation, identifier):
    return "raw/{namespace}/{identifier}*".format(namespace=_namespace(namespace),
                                                  identifier=identifier)


def _complement_dimensions(df):
    # data type does not exist, raise exception?
    existed_columns = set([x[0] for x in df.dtypes])
    for (n, t) in PARTITION_FIELDS:
        if n in existed_columns:
            continue
        df = df.withColumn(n, lit(None).cast(t()))
    return df


def _complement_identifiers(df, insert_identifier=None, delete_identifier=None):
    # no way to insert or delete same record many times
    existed_columns = set([x[0] for x in df.dtypes])
    if insert_identifier and 'insert_identifier' not in existed_columns:
        # insert happens firstly, so this check works well
        df = df.withColumn('insert_identifier', lit(insert_identifier).cast(LongType()))

    if not delete_identifier and 'delete_identifier' not in existed_columns:
        # add default value null when insert, because this value is always used for judge currrent row is valid or not
        df = df.withColumn('delete_identifier', lit(delete_identifier).cast(LongType()))

    if delete_identifier and 'delete_identifier' in existed_columns:
        # really delete happened, and we only record first delete time
        df = df.withColumn('delete_identifier_new',
                           when(df.delete_identifier is None, delete_identifier).otherwise(df.delete_identifier))\
            .drop(df.delete_identifier)\
            .withColumnRenamed('delete_identifier_new', 'delete_identifier')
    return df


def insert_unified_data(df, insert_identifier):
    df = _complement_dimensions(df)
    df = _complement_identifiers(df, insert_identifier=insert_identifier)
    partition_columns = [x[0] for x in PARTITION_FIELDS]
    df.write.mode("append").parquet("s3a://aaron-s3-poc/unified", partitionBy=partition_columns)
    return df


def delete_unified_data(df, delete_identifier):
    df = _complement_dimensions(df)
    df = _complement_identifiers(df, delete_identifier=delete_identifier)
    partition_columns = [x[0] for x in PARTITION_FIELDS]
    df.write.mode("overwrite").parquet("s3a://aaron-s3-poc/unified", partitionBy=partition_columns)
    return df
