import simplejson

from pyspark.sql.functions import udf, desc, when
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import posexplode

from utils.partition import s3key_of_rawfile_with_mildcard

# delete, update  lock


def parsed_result(raw_data, story_id):
    if raw_data:
        result = simplejson.loads(raw_data)['storePlatformData']['webexp-product']['results'][str(story_id)]
        label = result['label'].replace("\n", " ")
        appids = result['cardIds']
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


def insert(spark, params):
    namespace = params['namespace']
    manipulation = params['manipulation']
    identifier = params['identifier']
    file_path = s3key_of_rawfile_with_mildcard(namespace, manipulation, identifier)

    df = spark.read.format("com.databricks.spark.avro").load("s3a://aaron-s3-poc/" + file_path).cache()

    # df.select("id", "display_style").show()

    # keys = (df
    #         .select(explode("display_style"))
    #         .select("key")
    #         .distinct()
    #         .rdd.flatMap(lambda x: x)
    #         .collect())
    #
    # exprs = [col("display_style").getItem(k).alias(k) for k in keys]
    # print keys
    # print exprs

    # df.select(*exprs).show()

   df = spark.read.format("com.databricks.spark.avro").load("s3a://aaron-s3-poc/" + file_path).cache()
   _parsed_result_udf = udf(parsed_result, ArrayType(StringType()))
   _rank_udf =  udf(_rank, IntegerType())
   df1 = df.select("data_type", "data_granularity", "market_code", "device_code",
             "country_code", "year", "month", "day", "story_id", "url", "app_ids", "raw_data", posexplode("app_ids"))\
    .withColumn('parsed_result', _parsed_result_udf('raw_data', 'story_id'))\
    .withColumn("label", col("parsed_result")[0])\
    .withColumn("head", col("parsed_result")[1])\
    .withColumn("creative_urls", col("parsed_result")[2])\
    .drop("raw_data")\
    .drop("parsed_result")\
    .drop("app_ids")\
    .withColumnRenamed("col", "app_id")\
    .withColumn('story_app_rank', _rank_udf('pos'))\
    .drop('pos')\
    .cache()

   df1.show()
   # df1.show(truncate=False)
   # df1.printSchema()
   save_unified_data(df1, 20171127231509)
   # df1.write.mode("overwrite").parquet("s3a://aaron-s3-poc/unified/metrics/", partitionBy=["year", "month", "day", "market_code"])

   # from pyspark.sql.functions import *

   # df\
   #.withColumn('Id_New',when(df.Rank <= 5,df.Id).otherwise('other'))\
   #.drop(df.Id)\
   #.select(col('Id_New').alias('Id'),col('Rank'))\
   #.show()

   # keys = (df
   #         .select(explode("display_style"))
   #         .select("key")
   #         .distinct()
   #         .rdd.flatMap(lambda x: x)
   #         .collect())
   #
   # exprs = [col("display_style").getItem(k).alias(k) for k in keys]
   # print keys
   # print exprs

   # df.select(*exprs).show()

   # new_df = spark.read.json(df.rdd.map(lambda r: r.raw_data)).cache()
   # new_df = spark.read.json(df.select("raw_data").rdd).cache()
   # new_df.show()
   # new_df.printSchema()
   # new_df.selectExpr("pageData.id as pageDataId", "pageData.context.storefrontId as storeFrontId").show()
 

def delete():
    pass


def update():
    pass


def main(spark, params):
    manipulation = params["manipulation"]
    if manipulation == "insert":
        insert(spark, params)
