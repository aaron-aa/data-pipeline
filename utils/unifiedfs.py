import datetime
import hashlib
import json

import datetime

from pyspark.sql.types import NullType, LongType, IntegerType, StringType
from pyspark.sql.functions import lit, col, udf, concat_ws, when
from pyspark.storagelevel import StorageLevel

from metastore.manipulation import DELETE, INSERT, REFRESH, UPDATE
from metastore.event_type import LOAD
from metastore.event import S3
from utils.s3 import list as s3_list, rename as s3_rename, delete as s3_delete
from utils.retry import retry

BUCKET = "aaron-s3-poc"
ROOT = "unified"
TEMP = "__temporary__"

PARTITION_FIELDS = [
    ("data_granularity", StringType),
    ("date", StringType),
    ("market_code", StringType),
    ("ad_platform_id", IntegerType),
    ("os_code", StringType),
    ("device_code", StringType),
    ("device_species_code", StringType),
    ("manufacture_code", StringType),
    ("country_code", StringType),
    ("region_code", StringType),
    ("unified_category_id", IntegerType),
    ("category_id", IntegerType)
]
PARTITION_COLUMNS = [x[0] for x in PARTITION_FIELDS]


def write(evt, df, data_type, data_family):
    unamespace = _unified_namespace(data_type, data_family, evt.namespace)
    unified_df = globals()["_u" + evt.manipulation](df, unamespace, evt.identifier)

    unified_paths = unified_df.groupby(*tuple(PARTITION_COLUMNS)).count().drop("count")\
        .rdd.map(lambda r: BUCKET + "/" + _partition_path(unamespace, _partition_values(r))).collect()
    path_format_pairs = [(evt.path, evt.format), (",".join(unified_paths), "parquet")]
    _send_event(evt.namespace, evt.manipulation, evt.identifier, path_format_pairs)


def _uinsert(df, unamespace, identifier):
    df = _complete_dimensions(df)
    df = _complete_identifiers(df, insert_identifier=identifier, delete_identifier=None, update_identifier=None)
    df.write.mode("append").parquet(_s3key(unamespace),
                                    partitionBy=PARTITION_COLUMNS, compression="gzip")
    return df


def _uupdate(update_df, unamespace, identifier):
    where_df = update_df.select("where.*")
    where_df = _complete_dimensions(where_df)
    # group is to locate files
    partition_grouped_rows = where_df.rdd.map(lambda row: (_partition_values(row), row))\
        .aggregateByKey([], _merge_agg_value, _merge_agg_partition, numPartitions=1)\
        .collect()
    # non_partition_columns is to locate matched rows based on above files
    non_partition_columns = [x[0] for x in where_df.dtypes if x[0] not in PARTITION_COLUMNS]

    target_fields = update_df.select("set.*").collect()[0].asDict()["fields"]
    print target_fields
    target_values = update_df.select("value.*").collect()[0].asDict()

    unified_df = None

    for (group, rows) in partition_grouped_rows:
        print group, rows
        files_df = spark.read.parquet(_s3key(unamespace)).where(col("delete_id").isNull())
        ordered_fields = [x[0] for x in files_df.dtypes]
        for c, v in zip(PARTITION_COLUMNS, group):
            if v == None:
                files_df = files_df.where(col(c).isNull())
            else:
                where = "{}={}{}{}".format(c, "'" if isinstance(v, basestring) else "",
                                           v, "'" if isinstance(v, basestring) else "")
                files_df = files_df.where(where)

        unmatched_df = matched_df = files_df
        for c in non_partition_columns:
            in_ = set(map(lambda r: r[c], rows))
            if in_ == {None}:
                continue
            matched_df = matched_df.where(col(c).isin(in_))
            unmatched_df = unmatched_df.where(~ col(c).isin(in_))

        if matched_df is matched_df:
            result_df = _complete_identifiers(files_df, update_identifier=identifier)
            result_df = _update_fields(result_df, target_fields, target_values)
        else:
            matched_df = _complete_identifiers(matched_df, update_identifier=identifier)
            matched_df = _update_fields(matched_df, target_fields, target_values)
            result_df = unmatched_df.union(matched_df)

        if len(result_df.take(1)) == 0:
            # Nothing to update
            continue

        result_df = result_df.select(ordered_fields)
        _clean_s3_temp_data_files(identifier, UPDATE, unamespace)
        result_df.write.mode("append").parquet(_temp_s3key(identifier, UPDATE, unamespace),
                                               partitionBy=partition_columns,
                                               compression="gzip")
        _delete_s3_data_files(identifier, UPDATE, unamespace, group)
        partial_df = spark.read.parquet(_temp_s3key(identifier, UPDATE, unamespace)
                                        ).persist(StorageLevel.MEMORY_AND_DISK)
        partial_df.write.mode("append").parquet(_s3key(unamespace),
                                                partitionBy=partition_columns,
                                                compression="gzip")

        if not unified_df:
            unified_df = partial_df
        else:
            unified_df = unified_df.union(partial_df)
    return unified_df


def _udelete(where_df, unamespace, identifier):
    where_df = _complete_dimensions(where_df)
    # group is to locate files
    partition_grouped_rows = where_df.rdd.map(lambda row: (_partition_values(row), row))\
        .aggregateByKey([], _merge_agg_value, _merge_agg_partition, numPartitions=1)\
        .collect()
    # non_partition_columns is to locate matched rows based on above files
    non_partition_columns = [x[0] for x in where_df.dtypes if x[0] not in PARTITION_COLUMNS]

    unified_df = None

    for (group, rows) in partition_grouped_rows:
        print group, rows
        files_df = spark.read.parquet(_s3key(unamespace)).where(col("delete_id").isNull())
        ordered_fields = [x[0] for x in files_df.dtypes]
        for c, v in zip(PARTITION_COLUMNS, group):
            if v == None:
                files_df = files_df.where(col(c).isNull())
            else:
                where = "{}={}{}{}".format(c, "'" if isinstance(v, basestring) else "",
                                           v, "'" if isinstance(v, basestring) else "")
                files_df = files_df.where(where)

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
            result_df = unmatched_df.union(matched_df)

        if len(result_df.take(1)) == 0:
            # Nothing to delete
            continue

        result_df = result_df.select(ordered_fields)
        _clean_s3_temp_data_files(identifier, DELETE, unamespace)
        result_df.write.mode("append").parquet(_temp_s3key(identifier, DELETE, unamespace),
                                               partitionBy=partition_columns,
                                               compression="gzip")
        _delete_s3_data_files(identifier, DELETE, unamespace, group)
        partial_df = spark.read.parquet(_temp_s3key(identifier, DELETE, unamespace)
                                        ).persist(StorageLevel.MEMORY_AND_DISK)
        partial_df.write.mode("append").parquet(_s3key(unamespace),
                                                partitionBy=partition_columns,
                                                compression="gzip")

        if not unified_df:
            unified_df = partial_df
        else:
            unified_df = unified_df.union(partial_df)
    return unified_df


def _send_event(unamespace, manipulation, identifier, data_path_format_pairs):
    S3.send(LOAD, unamespace, manipulation, identifier, data_path_format_pairs)


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
                               when(col(identifier_name).isNull(), lit(identifier).cast(LongType())).otherwise(col(identifier_name)))\
                .drop(identifier_name)\
                .withColumnRenamed(temp, identifier_name)
    return df


def _s3key(unamespace):
    return "s3a://{}/{}/{}".format(BUCKET, ROOT, unamespace)


def _s3key_for_python(unamespace):
    return "{}/{}".format(ROOT, unamespace)


def _temp_s3key(identifier, manipulation, unamespace):
    return "s3a://{}/{}".format(BUCKET, _temp_s3key_for_boto(identifier, manipulation, unamespace))


def _temp_s3key_for_boto(identifier, manipulation, unamespace):
    return "{}/{}/{}/{}".format(TEMP, identifier, manipulation, unamespace)


def _partition_values(r):
    return tuple([r[x] for x in PARTITION_COLUMNS])


def _partition_path(unamespace, partition_values):
    source_keys = ["{}/{}".format(ROOT, unamespace)]
    for c, v in zip(PARTITION_COLUMNS, partition_values):
        if v == None:
            source_keys.append("{}=__HIVE_DEFAULT_PARTITION__".format(c))
        else:
            source_keys.append("{}={}".format(c, v))
    return "/".join(source_keys)


def _update_fields(df, target_fields, target_values):
    types = dict([(f.name, f.dataType) for f in df.schema.fields])
    for f in target_fields:
        df = df.drop(f).withColumn(f, lit(target_values[f]).cast(types[f]))
    return df


def _clean_s3_temp_data_files(identifier, manipulation, unamespace):
    temp_s3_key = _temp_s3key_for_boto(identifier, manipulation, unamespace)
    for from_key in s3_list(BUCKET, temp_s3_key):
        retry(s3_delete, (BUCKET, from_key))


def _delete_s3_data_files(identifier, manipulation, unamespace, partition_values):
    # delete existed keys
    prefix = _partition_path(unamespace, partition_values)
    for key in s3_list(BUCKET, prefix):
        retry(s3_delete, (BUCKET, key))


def _merge_agg_value(aggregated, v):
    aggregated.append(v)
    return aggregated


def _merge_agg_partition(aggregated1, aggregated2):
    return aggregated1 + aggregated2
