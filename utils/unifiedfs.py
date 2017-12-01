import datetime
import hashlib
import json

import datetime

from pyspark.sql.types import NullType, LongType, IntegerType, StringType
from pyspark.sql.functions import lit, col, udf, concat_ws, when
from pyspark.storagelevel import StorageLevel

from metastore.manipulation import DELETE, INSERT, REFRESH, UPDATE
from utils.s3 import list, rename, delete
from utils.retry import retry

BUCKET = "aaron-s3-poc"
ROOT = "unified"
TEMP = "__temporary__"

PARTITION_FIELDS = [
    ("data_granularity", StringType),
    ("date", StringType),
    ("market_code", StringType),
    ("ad_platform_id", StringType),
    ("os_code", StringType),
    ("device_code", StringType),
    ("device_species_code", StringType),
    ("manufacture_code", StringType),
    ("country_code", StringType),
    ("region_code", StringType),
    ("unified_category_id", StringType),
    ("category_id", StringType)
]
PARTITION_COLUMNS = [x[0] for x in PARTITION_FIELDS]


def write(df, namespace, data_type, data_family, manipulation, identifier):
    unamespace = _unified_namespace(data_type, data_family, namespace)
    return globals()["_u" + manipulation](df, unamespace, identifier)


def _unified_namespace(data_type, data_family, namespace):
    return "{}.{}.{}".format(data_type, data_family, namespace)


def _complete_dimensions(df):
    existed_columns = set([x[0] for x in df.dtypes])
    for (n, t) in PARTITION_FIELDS:
        if n in existed_columns:
            continue
        df = df.withColumn(n, lit(None).cast(t()))
    return df


def _complete_identifiers(df, insert_identifier=None, delete_identifier=None, update_identifier=None):
    # no way to insert or delete same record many times
    existed_columns = set([x[0] for x in df.dtypes])
    if insert_identifier and 'insert_id' not in existed_columns:
        # insert happens firstly, so this check works well
        df = df.withColumn('insert_id', lit(insert_identifier).cast(LongType()))

    for (identifier, identifier_name) in [(delete_identifier, 'delete_id'),
                                          (update_identifier, 'update_id')]:
        if not identifier and identifier_name not in existed_columns:
            # add default null, because this value is always used for judge currrent row is deleted or updated
            df = df.withColumn(identifier_name, lit(None).cast(LongType()))

        if identifier and identifier_name in existed_columns:
            # really delete happened, and we only record first delete time
            temp = '_{}'.format(identifier_name)
            df = df.withColumn(temp,
                               when(col(identifier_name).isNull(), identifier).otherwise(col(identifier_name)))\
                .drop(identifier_name)\
                .withColumnRenamed(temp, identifier_name)
    return df


def _s3key(unamespace):
    return "s3a://{}/{}/{}".format(BUCKET, ROOT, unamespace)


def _temp_s3key(identifier, manipulation, unamespace):
    return "s3a://{}/{}".format(BUCKET, _temp_s3key_for_boto(identifier, manipulation, unamespace))


def _temp_s3key_for_boto(identifier, manipulation, unamespace):
    return "{}/{}/{}/{}".format(TEMP, identifier, manipulation, unamespace)


def _uinsert(df, namespace, identifier):
    df = _complete_dimensions(df)
    df = _complete_identifiers(df, insert_identifier=identifier, delete_identifier=None, update_identifier=None)
    df.write.mode("append").parquet(_s3key(namespace),
                                    partitionBy=PARTITION_COLUMNS, compression="gzip")
    df.show()
    return df


def _partition_values(r):
    return tuple([r[x] for x in PARTITION_COLUMNS])


def _udelete(where_df, namespace, identifier):
    where_df = _complete_dimensions(where_df)
    # group is to locate files
    partition_grouped_rows = where_df.rdd.map(lambda row: (_partition_values(row), row))\
        .aggregateByKey([], _merge_agg_value, _merge_agg_partition, numPartitions=1)\
        .collect()
    # non_partition_columns is to locate matched rows based on above files
    non_partition_columns = [x[0] for x in where_df.dtypes if x[0] not in PARTITION_COLUMNS]

    for (group, rows) in partition_grouped_rows:
        print group, rows
        files_df = spark.read.parquet(_s3key(namespace))
        files_df.show(truncate=False)
        for c, v in zip(PARTITION_COLUMNS, group):
            if v == None:
                files_df = files_df.where(col(c).isNull())
            else:
                where = "{}={}{}{}".format(c, "'" if isinstance(v, basestring) else "",
                                           v, "'" if isinstance(v, basestring) else "")
                files_df = files_df.where(where)
        print "*" * 20, "files_df"
        files_df.show(truncate=True)

        unmatched_df = matched_df = files_df
        for c in non_partition_columns:
            in_ = set(map(lambda r: r[c], rows))
            if in_ == {None}:
                continue
            matched_df = matched_df.where(col(c).isin(in_))
            unmatched_df = unmatched_df.where(~ col(c).isin(in_))

        if matched_df is matched_df:
            result_df = _complete_identifiers(files_df, delete_identifier=identifier)
        else:
            matched_df = _complete_identifiers(matched_df, delete_identifier=identifier)
            print "*" * 20, "matched_df"
            matched_df.show(truncate=True)
            print "*" * 20, "unmatched_df"
            unmatched_df.show(truncate=True)
            result_df = unmatched_df.union(matched_df)
            result_df.persist(StorageLevel.MEMORY_AND_DISK)
        print "*" * 20, "result_df"
        result_df.show(truncate=True)

        _clean_temp_s3_files(identifier, DELETE, namespace)
        result_df.write.mode("append").parquet(_temp_s3key(identifier, DELETE, namespace),
                                               partitionBy=partition_columns,
                                               compression="gzip")
        _mv_s3_files(identifier, DELETE, namespace, group)


def _clean_temp_s3_files(identifier, manipulation, unamespace):
    temp_s3_key = _temp_s3key_for_boto(identifier, manipulation, unamespace)
    for from_key in list(BUCKET, temp_s3_key):
        retry(delete, (BUCKET, from_key))


def _mv_s3_files(identifier, manipulation, unamespace, group):
    source_keys = ["{}/{}".format(ROOT, unamespace)]
    for c, v in zip(PARTITION_COLUMNS, group):
        if v == None:
            source_keys.append("{}=__HIVE_DEFAULT_PARTITION__".format(c))
        else:
            source_keys.append("{}={}".format(c, v))
    prefix = "/".join(source_keys)
    for from_key in list(BUCKET, prefix):
        retry(delete, (BUCKET, from_key))

    temp_s3_key = _temp_s3key_for_boto(identifier, manipulation, unamespace)
    for from_key in list(BUCKET, temp_s3_key):
        to_key = "/".join(from_key.split("/")[len(temp_s3_key.split("/")):])
        retry(rename, (BUCKET, from_key, to_key))


def _merge_agg_value(aggregated, v):
    aggregated.append(v)
    return aggregated


def _merge_agg_partition(aggregated1, aggregated2):
    return aggregated1 + aggregated2
