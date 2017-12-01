import simplejson

from pyspark.sql.functions import udf, desc, when
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import posexplode

from utils.unifiedfs import write
from metastore.event import S3
from metastore.event_type import TRANSFORM
# delete, update  lock


def update():
    pass


def delete(spark, params):
    namespace = params['namespace']
    manipulation = params['manipulation']
    identifier = params['identifier']
    events = S3.receive(spark, TRANSFORM, namespace, manipulation, identifier)
    events.show(truncate=False)
    for evt in events.collect():
        _transform_for_delete(evt, namespace, manipulation, identifier)


def insert(spark, params):
    namespace = params['namespace']
    manipulation = params['manipulation']
    identifier = params['identifier']

    events = S3.receive(spark, TRANSFORM, namespace, manipulation, identifier)
    events.show(truncate=False)
    for evt in events.collect():
        _transform_for_insert(evt, namespace, manipulation, identifier)


def _transform_for_delete(evt, namespace, manipulation, identifier):
    if evt.format == 'avro':
        df = spark.read.format("com.databricks.spark.avro").load("s3a://{}".format(evt.path)).cache()
    df.show(truncate=False)
    write(df, namespace, "dimension", "product", manipulation, identifier)


def _transform_for_insert(evt, namespace, manipulation, identifier):
    if evt.format == 'avro':
        df = spark.read.format("com.databricks.spark.avro").load("s3a://{}".format(evt.path)).cache()

    _parsed_udf = udf(_parsed_response, ArrayType(StringType()))
    dimension_df = df.select("featured_story_id", "data_granularity", "market_code", "device_code",
                             "country_code", "date", "url", "product_ids", "rank", "raw_data", "data_name")\
        .withColumn('parsed_result', _parsed_udf('raw_data', 'featured_story_id'))\
        .withColumn("label", col("parsed_result")[0])\
        .withColumn("head", col("parsed_result")[1])\
        .withColumn("creative_urls", col("parsed_result")[2])\
        .drop("raw_data")\
        .drop("parsed_result").cache()
    dimension_df.show(truncate=False)
    write(dimension_df, namespace, "dimension", "product", manipulation, identifier)

    product_metrics_df = dimension_df.select("featured_story_id", "data_granularity", "market_code",
                                             "device_code", "country_code", "date", "product_ids", "rank", posexplode("product_ids"), "data_name")\
        .drop("product_ids")\
        .withColumnRenamed("rank", "featured_story_rank")\
        .withColumnRenamed("col", "product_id")\
        .withColumn('featured_story_product_rank', col('pos') + 1)\
        .drop('pos')\
        .cache()
    product_metrics_df.show(truncate=False)
    write(product_metrics_df, namespace, "metrics", "product", manipulation, identifier)


def _parsed_response(raw_data, featured_story_id):
    if raw_data:
        result = simplejson.loads(raw_data)['storePlatformData']['webexp-product']['results'][str(featured_story_id)]
        label = result['label'].replace("\n", " ")
        productids = result['cardIds']
        head = result.get('editorialNotes', {}).get('name')
        description = None
        content = None
        display_style = {
            "displayStyle": result.get('displayStyle'),
            "cardDisplayStyle": result.get('cardDisplayStyle'),
            "displaySubStyle": result.get('displaySubStyle')
        }
        creative_urls = result.get('editorialArtwork', {}).get('dayCard', {}).get('url')\
            .replace("{w}x{h}{c}.{f}", "1024x1024.jpg")
        return [label, head, creative_urls]
    else:
        return [None, None, None]


def display_style(raw_data):
    return None


def main(spark, params):
    manipulation = params["manipulation"]
    if manipulation == "insert":
        insert(spark, params)


params_insert = {
    "namespace": "ss.featured_story.v1",
    "manipulation": "insert",
    "identifier": "20171130175127"
}

params_delete = {
    "namespace": "ss.featured_story.v1",
    "manipulation": "delete",
    "identifier": "20171130175127"
}
#insert(spark, params_insert)
# spark.read.parquet("s3a://aaron-s3-poc/unified/data_type=metrics").show()
# spark.read.parquet("s3a://aaron-s3-poc/unified/data_type=dimensions").show()


#delete(spark, params_delete)
insert(spark, params)

# spark.read.parquet("s3a://aaron-s3-poc/unified/data_type=metrics/primary_dimension=product/data_granularity=daily/date=2017-11-31/market_code=apple-store/device_code=iphone/country_code=US/data_name=featured_story/part-00001-0588d5ae-8768-4855-ac1f-cc3634bcf106.c000.snappy.parquet").show()

#df = spark.read.option("mergeSchema", "true").parquet("s3a://aaron-s3-poc/unified/data_type=metrics").cache()
# df.show()
#df.select("featured_story_id", "product_id", "delete_identifier", "insert_identifier").where("market_code='apple-store'").where("device_code='iphone'").where("delete_identifier IS NULL").show()
