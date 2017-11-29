import simplejson

from pyspark.sql.functions import udf, desc, when
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import posexplode

from utils.partition import s3key_of_rawfile_with_mildcard, insert_unified_data

# delete, update  lock


def update():
    pass


def delete():
    namespace = params['namespace']
    manipulation = params['manipulation']
    identifier = params['identifier']
    file_path = s3key_of_rawfile_with_mildcard(namespace, manipulation, identifier)
    df = spark.read.format("com.databricks.spark.avro").load("s3a://aaron-s3-poc/" + file_path).cache()


def insert(spark, params):
    namespace = params['namespace']
    manipulation = params['manipulation']
    identifier = params['identifier']
    file_path = s3key_of_rawfile_with_mildcard(namespace, manipulation, identifier)
    df = spark.read.format("com.databricks.spark.avro").load("s3a://aaron-s3-poc/" + file_path).cache()

    _parsed_udf = udf(_parsed_result, ArrayType(StringType()))
    dimension_df = df.select("featured_story_id", "data_granularity", "market_code", "device_code",
                             "country_code", "date", "url", "product_ids", "rank", "raw_data")\
        .withColumn('parsed_result', _parsed_udf('raw_data', 'featured_story_id'))\
        .withColumn("label", col("parsed_result")[0])\
        .withColumn("head", col("parsed_result")[1])\
        .withColumn("creative_urls", col("parsed_result")[2])\
        .withColumn("data_type", lit("dimensions").cast(StringType()))\
        .withColumn("data_name", lit(_data_name(namespace)).cast(StringType()))\
        .withColumn("primary_dimension", lit('product').cast(StringType()))\
        .drop("raw_data")\
        .drop("parsed_result").cache()
    dimension_df.show(truncate=False)
    insert_unified_data(dimension_df, identifier)

    product_metrics_df = dimension_df.select("featured_story_id", "data_granularity", "market_code",
                                             "device_code", "country_code", "date", "product_ids", "rank", posexplode("product_ids"))\
        .drop("product_ids")\
        .withColumnRenamed("rank", "featured_story_rank")\
        .withColumnRenamed("col", "product_id")\
        .withColumn('featured_story_product_rank', col('pos') + 1)\
        .withColumn("data_type", lit("metrics").cast(StringType()))\
        .withColumn("data_name", lit(_data_name(namespace)).cast(StringType()))\
        .withColumn("primary_dimension", lit('product').cast(StringType()))\
        .drop('pos')\
        .cache()
    product_metrics_df.show(truncate=False)
    insert_unified_data(product_metrics_df, identifier)


def _data_name(namespace):
    ns = namespace.split(".")[1: -1]
    if "v1" in ns:
        ns.remove("v1")
    return "_".join(ns)


def _parsed_result(raw_data, featured_story_id):
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
