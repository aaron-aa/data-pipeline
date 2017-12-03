from extract.schema import get


def update(schema_alias, filters, df):
    delete(schema_alias, filters)
    insert(schema_alias, df)


def delete(schema_alias, filters):
    # country_code=? date=? ..., must exist in schema
    pass


def insert(schema_alias, df):
    """
    """
    schema_ = get(schema_alias)
    path = root(schema_, "insert", "avro")
    if _is_valiad_schema(schema_, df):
        data.write.mode('overwrite').format("com.databricks.spark.avro").save(path)
    else:
        raise Exception("dataframe schema is not same as the defined")


def _is_valiad_schema(schema_, df):
    """
    1. type check
    [('title', 'string'), ('air_date', 'string'), ('doctor', 'int')]

    2. value check
    from pyspark.sql.functions import *
    df = sc.parallelize([("iphone", 100, 'title 1'),("ipad", 501, 'title2'),("windows-phone", 100, 'title 3 3333'),("test", 1000, 'title 4')]).toDF(["device_code", "free_download", "app_title"])
    df.select("*", ((length("app_title") < lit(10)) & (col("free_download") <= lit(500)) & (col("device_code").isin('iphone', 'ipad'))).alias("evaluation")).show()

    +-------------+-------------+------------+----------+
    |  device_code|free_download|   app_title|evaluation|
    +-------------+-------------+------------+----------+
    |       iphone|          100|     title 1|      true|
    |         ipad|          501|      title2|     false|
    |windows-phone|          100|title 3 3333|     false|
    |         test|         1000|     title 4|     false|
    +-------------+-------------+------------+----------+
    """
    dtypes = df.dtypes
    return sorted(schema_["schema"]['fields'].keys()) == sorted(map(lambda x: x[0], dtypes))
