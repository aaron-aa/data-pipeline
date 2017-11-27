from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("ES Test")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

q = """{
    "type" : { "value" : "log" },
    "query": {
        "match_all": {}
    }
}"""

edf = sqlContext.read.format("org.elasticsearch.spark.sql")\
    .option("es.nodes", "localhost")\
    .option("es.port", "9200")\
    .option("es.query", q)\
    .option("es.resource", "filebeat-*")\
    .option("es.scroll.limit", 1)\
    .option("es.read.field.as.array.include", "tags")\
    .load()\
    .select(["@timestamp", "offset", "beat", "message", "tags"])\
    .limit(3)

# Copyright (c) 2015 App Annie Inc. All rights reserved.
import copy
import sys
import time

from itertools import product
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionTimeout, ConnectionError
from pyspark.sql import Row

"""
  _______________________________
/ Script scope constants and util \
\ functions                       /
  -------------------------------
         \   ^__^
          \  (oo)\_______
             (__)\       )\/\
                 ||----w |
                 ||     ||
"""
PARALLELISM = 10

INCREMENTAL_DAY = "2000-01-01"


def _float6(value):
    return float("{0:.6f}".format(value))


def _range_type_id(range_type):
    from common.python.aso_utils import RangeType
    return RangeType[range_type].id


def _is_incremental(range_type):
    from common.python.aso_utils import RangeType
    return RangeType[range_type].name == RangeType.INCREMENTAL.name


def _parallelism(params):
    return int(params.get("parallelism", PARALLELISM))


def _device_id(platform_id, device_id):
    return platform_id * 1000 + device_id


def _func_name():
    return sys._getframe().f_code.co_name


def _schema(schema_name):
    from workflow.module.utils import load_schema
    return load_schema(schema_name)


def _devices(sc, platform_id):
    from common.python.aso_spark_utils import aso_int_platforms_devices_dataframe

    device_ids_rows = aso_int_platforms_devices_dataframe(sc).filter("int_platform = %s" % platform_id)\
        .select("int_device").collect()
    return map(lambda r: _device_id(platform_id, r.int_device), device_ids_rows)


def _month(day, params):
    return "".join(day.split("-")[:2])


def _year(day, params):
    return day.split("-")[0]


def _es(schema_name="ASO_TRAFFIC_SHARE_ES_O", timeout=10):
    from workflow.module.utils import get_es_connection_config
    from workflow.conf import settings

    schema = _schema(schema_name)
    conn_conf = get_es_connection_config(schema, 'es-py', settings)
    conn_conf["timeout"] = timeout
    return Elasticsearch(**conn_conf)


def _alias(idx):
    """
    To make life easily and not impact web api when "rebuilding" index, we make a rule:
    read data / find index through alias but write(insert/delete/merge) data on real index.
    """
    return "{}-alias".format(idx)


def _is_alias(alias):
    return "alias" in alias


def _aliases(schema_name, index_pattern, params):
    """
    Args:
        index_pattern: like aso-kpi-svi-*-2016
    Returns:
        [alias, ...] like ["aso-kpi-svi-us-2016-alias", "aso-kpi-svi-jp-2016-alias"]
    """

    es = _es(schema_name)
    # output like [{u'alias': u'aso-kpi-svi-co-2000-alias'}, ...], this is support from elasticsearch==5.2.0
    aliases = es.cat.aliases(_alias(index_pattern), h="alias")

    if 'include_countries' in params:
        include_countries = [c.lower() for c in params['include_countries'].split(",")]
        aliases = filter(lambda alias: any(c in alias['alias'] for c in include_countries), aliases)
    if 'exclude_countries' in params:
        exclude_countries = [c.lower() for c in params['exclude_countries'].split(",")]
        aliases = filter(lambda alias: all(c not in alias['alias'] for c in exclude_countries), aliases)

    return set(map(lambda x: x['alias'], aliases))


def _indices_of_alias(alias):
    if _is_alias(alias):
        return map(lambda x: x['index'], _es().cat.aliases(alias, h="index"))
    else:
        raise Exception("{} {}: {} is not alias".format(__file__, _func_name(), alias))


def _latest_index(alias):
    """
    One alias has one index, but we maybe meet inconsistent case that one alias has more than one indices associated
    with it. And ES cant not do insert/update/delete for this case, so we have to return the right index from multiple
    indices.
    """
    indices = _indices_of_alias(alias)
    if len(indices) == 1:
        return indices[0]
    else:
        return sorted(indices)[-1]


"""
 ________________________________
/ Below section is the private    \
\ interfaces for managing indices /
 --------------------------------
        \   ^__^
         \  (oo)\_______
            (__)\       )\/\
                ||----w |
                ||     ||
"""


class _IndexUpdater(object):
    # Keep old index, create new index and insert data to it, alias new index to old index alias, delete old index
    RENAME = "rename"
    # Delete documents then insert documents with same index, this is the default mode
    UPDATE = "update"

    def __init__(self, params):
        self.params = params

    def is_rename_mode(self):
        return self.id() and self.mode() == self.RENAME

    def id(self):
        """
        id format must be %Y%m%d%H%M
        """
        from common.python.aso_utils import to_date
        id_ = self.params.get("refresh_id", None)
        if id_:
            try:
                # check id format
                to_date(id_, fmt='%Y%m%d%H%M')
            except Exception as e:
                raise
        return id_

    def mode(self):
        return self.params.get("refresh_mode", self.UPDATE)


def _index_postfix(schema_name, day, params):
    schema = _schema(schema_name)
    postfix_name = schema['table'].split("$")[-1]
    postfix = globals()["_{}".format(postfix_name)](day, params)

    updater = _IndexUpdater(params)
    if updater.is_rename_mode():
        postfix += updater.id()

    return postfix_name, postfix


def _segments_count(es, index):
    stats = es.indices.stats(index=index, metric="segments")
    return stats["indices"][index]["primaries"]["segments"]["count"]


def _force_merge(index_patterns, params):
    """
    Time consuming and heavy opration, please be careful!
    This is the implementation of shrinking index size when the index has been updated(delete then insert).
    we only expunge segments with deletes in it. In Lucene, a document is not deleted from a segment,
    just marked as deleted
    Be aware that,
    1. Call this method in one blocking process. Because merge triggered by forcemerge API are not throttled at all in
    ES side. They can consume all of the I/O on your nodes, leaving nothing for search and potentially making your
    cluster unresponsive, so we have to merge one index once.
    If we have 50 indices to merge, this func will cost about 50 * 5 = 250 minutes. So for saving cost,
    you'd better call it in a 2 nodes AMR cluster which is not the one that save/update data into ES.
    2. Execute this func when no writes request to ES, because merge will hurt the dynamic index - an index
    that is being actively updated.
    Args:
        index_patterns: ["aso-kpi-ts-*-201612", "aso-kpi-ts-*-201701"]
    """

    schema_name = params["schema"]
    es = _es(schema_name, timeout=60)

    # To merge indices in order, it's easy for monitoring through marvel
    indices = sorted([_latest_index(alias) for p in index_patterns for alias in _aliases(schema_name, p, params)],
                     reverse=True)

    while len(indices) > 0:
        index = indices.pop()

        if _segments_count(es, index) > 1:
            try:
                es.indices.forcemerge(index=index,
                                      max_num_segments=1,
                                      ignore_unavailable=True,
                                      only_expunge_deletes=False,
                                      flush=True,
                                      wait_for_merge=False)
            except Exception as e:
                if isinstance(e, ConnectionTimeout) or "timeout" in e.message.lower():
                    # Ignore timeout
                    pass
                elif isinstance(e, ConnectionError) or "connectionerror" in e.message.lower():
                    # 1.This means connection aborted, keep_alive run out of it's life.
                    # 2.Create connection when required, to avoid creating connection in each loop, which is dangerous
                    es = _es(schema_name, timeout=60)
                else:
                    raise

            # Block current porcess until merge is done
            finished = False
            while not finished:
                finished = _segments_count(es, index) == 1
                # Wait 2 minutes even if merge is done, to make sure merge process of ES release all resources.
                # I think it's a good practice because it worked well in 3 days long merge jobs.
                time.sleep(60 * 2)


def _rename_index(new_index_patterns, new_index_indicator, params):
    """
    Dangerous opration, please be careful!
    This function is to solve below case.
    1. An existed index,
        aso-kpi-ts-us-201611 <- aso-kpi-ts-us-201611-alias(formal index)
    2. We'd like to refresh historical data caused by some fixes for the above index effectively, so we can not update
    the origin index itself, we have to create an new index and replace it later,
        aso-kpi-ts-us-201611fix <- aso-kpi-ts-us-201611fix-alias(temporary new alias)
    3. Add formal alias to new index
        curl -XPOST /_aliases -d
        '{
            "actions" : [
                { "add" : { "index" : "aso-kpi-ts-us-201611fix", "alias" : "aso-kpi-ts-us-201611-alias" } }
            ]
        }'
    4. Remove old index
        curl -XDELETE aso-kpi-ts-us-201611
    5. Delete new alias for new index
        curl -XPOST /_aliases -d
        '{
            "actions" : [
                { "remove" : { "index" : "aso-kpi-ts-us-201611fix", "alias" : "aso-kpi-ts-us-201611fix-alias" } }
            ]
        }'
    1 & 2 happend when we store index to ES, 3 & 4 & 5 would be handled by this func.
    Args:
        new_index_patterns: ["aso-kpi-ts-*-201612201702170432",...], pattern of the new index to replace old index
        new_index_indicator: the indicator to distinguash old index & new index, like "201702170432"
    """
    from common.python.aso_utils import retry

    schema_name = params["schema"]
    es = _es(schema_name)
    helper = es.indices

    for p in new_index_patterns:
        aliases = _aliases(schema_name, p, params)
        for new_alias in aliases:
            new_index = _latest_index(new_alias)
            formal_alias = new_alias.replace(new_index_indicator, "")
            retry(helper.put_alias, (new_index, formal_alias))

            old_indices = _indices_of_alias(formal_alias)
            for old_index in old_indices:
                # Ensure latest index would not be deleted by mistake, and ensure alias point to 1 index at least,
                # which help us would not impact user experience
                if old_index != new_index:
                    retry(helper.delete, (old_index,))

            # Delete all non latest indices with new alias in case
            for non_latest_index in (set(_indices_of_alias(new_alias)) - set([new_index])):
                retry(helper.delete, (non_latest_index,))

            # Execute this delete lastly is to help us rerun the above logic when failure happened, because new alias
            # is not deleted yet
            retry(helper.delete_alias, (new_index, new_alias))


"""
__________________________________________
/ Below section is the private interfaces  \
| for deleting documents by day, device,   |
| and country optional specified in        |
| params. These are the only three         |
\ supported dimensions                     /
------------------------------------------
      \   ^__^
       \  (oo)\_______
          (__)\       )\/\
              ||----w |
              ||     ||
"""


def _definitions_of_existed_docs(day, devices, schema_name, params):
    from workflow.module.utils import replace_params

    schema = _schema(schema_name)
    index_pattern = replace_params(schema['table'], params)
    indices = _aliases(schema_name, index_pattern, params)

    doc_type = replace_params(schema['doc_type'], params)

    doc_id_queries = [
        {
            "fields": [],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"day": day}},
                        {"term": {"device": device}}
                    ]
                }
            }
        }
        for device in devices]

    return [Row(schema_name=schema_name, index_name=index_name, doc_type=doc_type, doc_id_query=doc_id_query)
            for index_name, doc_id_query in product(indices, doc_id_queries)]


def _del_doc(definition, docs_per_batch=3000, request_timeout=90):
    from common.python.aso_utils import chunks

    index_name = _latest_index(definition.index_name)
    doc_type = definition.doc_type
    doc_id_query = definition.doc_id_query
    schema_name = definition.schema_name

    es = _es(schema_name)
    hits = helpers.scan(es, query=doc_id_query, index=index_name, doc_type=doc_type,
                        size=docs_per_batch, request_timeout=request_timeout, ignore_unavailable=True)

    for chunk in chunks(hits, docs_per_batch):
        try:
            actions = [{'_op_type': 'delete',
                        '_index': index_name,
                        '_type': doc_type,
                        '_id': hit["_id"]
                        } for hit in chunk]
            helpers.bulk(es, actions, chunk_size=docs_per_batch, request_timeout=request_timeout)
        except helpers.ElasticsearchException as e:
            if isinstance(e, helpers.BulkIndexError):
                # BulkIndexError: (u'10 document(s) failed to index.', [{u'delete': {u'status': 404, u'_type': u'1',
                # u'_shards': {u'successful': 2, u'failed': 0, u'total': 2}, u'_index': u'aso-kpi-ts-fi-201610',
                # u'_version': 6, u'found': False, u'_id': u'AVfC2F7BHqX2ok1atK9Q'}},...]
                non_404s = filter(lambda x: x.get('delete', {}).get('status') != 404, e.errors)
                # Ignore 404
                if non_404s:
                    raise e
            else:
                raise e
        finally:
            es.indices.refresh(index_name)


def _del_existed_docs(sc, schema_name, day, platform_id, params):
    """
    ES just mark documents as deleted, documents are not deleted from segment. You could call _force_merge to
    delete them permantly.
    """
    devices = _devices(sc, platform_id)
    definitions = _definitions_of_existed_docs(day, devices, schema_name, params)
    if definitions:
        sc.sparkContext.parallelize(definitions, min(_parallelism(params), len(definitions))).foreach(_del_doc)


"""
________________________________________
/ Below section is the public interfaces \
\ for storing documents to ES            /
----------------------------------------
      \   ^__^
       \  (oo)\_______
          (__)\       )\/\
              ||----w |
              ||     ||
"""


def _ts_es_row(key_values):
    key = key_values[0]
    values = sorted(key_values[1], reverse=True, key=lambda v: v[1])

    return Row(country_code=key[0], month=key[1], device=key[2], day=key[3],
               keyword_id=key[4], app_ids=[v[0] for v in values], values=[v[1] for v in values])


def store_aso_traffic_share_es_o(sc, params):
    from common.python.aso_utils import Platform
    from common.python.aso_spark_utils import (merge_agg_value as combine, merge_agg_partition as merge,
                                               filter_column_by_include_or_exclude)
    from workflow.module.utils import load_data_to_spark_dataframe, store_spark_dataframe

    es_params = copy.deepcopy(params)

    platform_id = Platform[params["platform"]].id
    es_params['platform_id'] = platform_id
    params['platform_id'] = platform_id

    day = params["date"]
    postfix_name, postfix = _index_postfix(params["schema"], day, params)
    es_params[postfix_name] = postfix

    doc_type = str(_range_type_id(params["range_type"]))
    es_params["range_type_id"] = doc_type

    # Delete all existed documents
    es_params["country_code"] = "*"
    _del_existed_docs(sc, "ASO_TRAFFIC_SHARE_ES_O", day, platform_id, es_params)

    # Store traffic share to ES
    traffic_share = load_data_to_spark_dataframe(sc, 'ASO_KEYWORD_KPI_TS_O', params)
    traffic_share = filter_column_by_include_or_exclude(traffic_share, params)\
        .select("platform_id", "device_id", "country_code", "keyword_id", "app_id", "traffic_share")\
        .rdd.map(lambda r: ((r.country_code, postfix, _device_id(r.platform_id, r.device_id), day, r.keyword_id),
                            (r.app_id, _float6(r.traffic_share))))\
        .filter(lambda r: r[1][1] > 0)\
        .aggregateByKey([], combine, merge)\
        .map(lambda kv: _ts_es_row(kv))\
        .toDF().select("country_code", postfix_name, "device", "day", "keyword_id", "app_ids", "values")

    store_spark_dataframe(sc,
                          traffic_share,
                          "ASO_TRAFFIC_SHARE_ES_O|range_type_id={}".format(doc_type),
                          params={"doc_id_column": None, "docs_per_batch": 3000, "_refresh_index": True},
                          coalesce=_parallelism(params))


def _validate_params_when_svi_stored_partially(params):
    from common.python.aso_utils import ASOKPI
    if params["kpi"] == ASOKPI.SVI:
        country_check = "include_countries" in params or "exclude_countries" in params
        source_check = "source" in params
        if not (country_check and source_check):
            raise Exception("For SVI, no (include_countries or exclude_countries) and no source "
                            "is specified in params: {}".format(params))


def store_aso_keyword_kpi_es_o(sc, params):
    from common.python.aso_utils import Platform, ASO_KEYWORD_KPI_SCHEMA
    from common.python.aso_spark_utils import filter_column_by_include_or_exclude
    from workflow.module.utils import load_data_to_spark_dataframe, store_spark_dataframe

    _validate_params_when_svi_stored_partially(params)

    es_params = copy.deepcopy(params)

    platform_id = Platform[params["platform"]].id
    es_params['platform_id'] = platform_id
    params['platform_id'] = platform_id

    range_type = params["range_type"]
    doc_type = str(_range_type_id(range_type))
    es_params["range_type_id"] = doc_type

    day = INCREMENTAL_DAY if _is_incremental(range_type) else params["date"]
    postfix_name, postfix = _index_postfix(params["schema"], day, params)
    es_params[postfix_name] = postfix

    kpi_name = params["kpi"].lower()
    es_params["kpi"] = kpi_name

    # Delete all existed documents
    es_params["country_code"] = "*"
    _del_existed_docs(sc, "ASO_KEYWORD_KPI_ES_O", day, platform_id, es_params)

    # Store kpi to ES
    columns = ("kpi", "country_code", postfix_name, "device", "day", "keyword_id", "value")
    kpi_schema = ASO_KEYWORD_KPI_SCHEMA[params["kpi"]][range_type]

    keyword_kpi = load_data_to_spark_dataframe(sc, kpi_schema, params)\
        .select("platform_id", "device_id", "country_code", "keyword_id", "kpi_value")\
        .rdd.map(lambda r: (kpi_name, r.country_code, postfix,
                            _device_id(r.platform_id, r.device_id),
                            day, r.keyword_id,
                            _float6(r.kpi_value)))\
        .toDF(list(columns)).select(*columns)
    keyword_kpi = filter_column_by_include_or_exclude(keyword_kpi, params)

    store_spark_dataframe(sc,
                          keyword_kpi,
                          "ASO_KEYWORD_KPI_ES_O|range_type_id={},kpi={}".format(doc_type, kpi_name),
                          params={"doc_id_column": None, "docs_per_batch": 3000, "_refresh_index": True},
                          coalesce=_parallelism(params))


"""
________________________________________
/ Below section is the public interfaces \
\ for managing indices                   /
----------------------------------------
       \   ^__^
        \  (oo)\_______
           (__)\       )\/\
               ||----w |
               ||     ||
"""


def _index_patterns(params):
    from common.python.aso_utils import RangeType, days_in_period_by_range, str_date
    from workflow.module.utils import replace_params

    from_date = params["from_date"]
    to_date = params["to_date"]
    days = map(lambda d: str_date(d), days_in_period_by_range(RangeType.DAY.name, from_date, to_date))

    schema_name = params["schema"]
    postfixes = set(map(lambda day: _index_postfix(schema_name, day, params), days))

    schema = _schema(schema_name)
    for postfix_name, postfix in postfixes:
        cparams = copy.deepcopy(params)
        cparams[postfix_name] = postfix
        cparams["country_code"] = "*"
        yield replace_params(schema['table'], cparams).lower()


def rename_index(sc, params):
    """
    Does not support rename mode for SVI and KD
    1) not that slow even we use udpate mode
    2) they only can be rename when we refresh 1 * n year data
    3) like svi have several job like svi of apple and svi of google to update index, it's kind of complicated
    """
    from common.python.aso_utils import ASOKPI

    assert (params["kpi"] in (ASOKPI.TS))

    updater = _IndexUpdater(params)
    if updater.is_rename_mode():
        index_patterns = _index_patterns(params)
        if index_patterns:
            _rename_index(index_patterns, updater.id(), params)


def force_merge(sc, params):
    index_patterns = _index_patterns(params)
    if index_patterns:
        _force_merge(index_patterns, params)


def main(sc, params):
    from common.python.aso_spark_utils import canned_acl

    sc = canned_acl(sc)

    cparams = copy.deepcopy(params)
    if "function" in cparams:
        function = cparams["function"]
    else:
        function = "store_" + cparams["schema"].lower()

    globals()[function](sc, cparams)

    sc.stop()
