import datetime
import hashlib
import json

from pyspark.sql.types import NullType, LongType, IntegerType
from pyspark.sql.functions import lit

#"granularity": "daily",  # daily/2017-11-24/None/, hourly/2017-11-24/23/, yearly/2017-01-01/None/, monthly/2017-01-31/None,

PARTITION_FIELDS = [
    ("data_type", StringType),
    ("data_granularity", StringType),
    ("year", IntegerType),
    ("month", IntegerType),
    ("day", IntegerType),
    ("hour", IntegerType),
    ("market_code", StringType),
    ("device_code", StringType),
    ("country_code", StringType),
    ("category_id", LongType),
    ("ad_platform_id", LongType)
]


def signature(data):
    if data:
        md5 = hashlib.md5(json.dumps(data)).hexdigest()
        return "dp{}".format(md5)
    else:
        return ""


def data_partition_name(key):
    return "-".join(map(str, key)) if key else ""


def identifier():
    return datetime.datetime.today().strftime('%Y%m%d%H%M%S')


def _namespace(ns):
    return ns.replace(".", "/")


def s3key_of_rawfile(namespace, manipulation, identifier, signature, data_partition_name, format):
    """
    ss/story/v1/insert/20171127171302-dpa9d9b034887206139fba4dd18d925c6d-2017-11-31-apple-store-iphone-JP.avro
    ss/story/v1/insert/20171127171302-dpa9d9b034887206139fba4dd18d925c6d-2017-11-31-apple-store-iphone-US.avro
    """
    key = "{namespace}/{manipulation}/{identifier}" + \
        "{signature_sep}{signature}{signature_sep}{data_partition_name}.{format}"
    return key.format(namespace=_namespace(namespace),
                      identifier=identifier,
                      format=format,
                      manipulation=manipulation,
                      signature=signature,
                      signature_sep="-" if signature else "",
                      data_partition_name=data_partition_name)


def s3key_of_rawfile_with_mildcard(namespace, manipulation, identifier):
    return "{namespace}/{manipulation}/{identifier}*".format(namespace=_namespace(namespace),
                                                             identifier=identifier, manipulation=manipulation)


def save_unified_data(df, insert_identifier=None, delete_identifier=None):
    existed_columns = set([x[0] for x in df.dtypes])
    for (n, t) in PARTITION_FIELDS:
        if n in existed_columns:
            continue
        df = df.withColumn(n, lit(None).cast(t()))
    if "insert_identifier" not in existed_columns:
        df = df.withColumn('insert_identifier', lit(insert_identifier).cast(LongType()))
    if "insert_identifier" not in existed_columns:
        df = df.withColumn('delete_identifier', lit(delete_identifier).cast(LongType()))

    df.show(truncate=False)
    df.printSchema()
    partition_columns = [x[0] for x in PARTITION_FIELDS]
    df.write.mode("overwrite").parquet("s3a://aaron-s3-poc/unified/metrics/", partitionBy=partition_columns)
    return df


def save_unified_data(df, insert_identifier=None, delete_identifier=None):
    existed_columns = set([x[0] for x in df.dtypes])
    for (n, t) in PARTITION_FIELDS:
        if n in existed_columns:
            continue
        df = df.withColumn(n, lit(None).cast(t()))
    if "insert_identifier" not in existed_columns:
        df = df.withColumn('insert_identifier', lit(insert_identifier).cast(LongType()))
    if "insert_identifier" not in existed_columns:
        df = df.withColumn('delete_identifier', lit(delete_identifier).cast(LongType()))

    df.show(truncate=False)
    df.printSchema()
    partition_columns = [x[0] for x in PARTITION_FIELDS]
    df.write.mode("overwrite").parquet("s3a://aaron-s3-poc/unified/metrics/", partitionBy=partition_columns)
    return df
