import datetime

#"granularity": "daily",  # daily/2017-11-24/None/, hourly/2017-11-24/23/, yearly/2017-01-01/None/, monthly/2017-01-31/None,

FIELDS = ["data_type", "data_granularity", "year", "month", "day", "hour", "market_code",
          "device_code", "country_code", "category_id", "ad_platform_id"]


def root(schema, operation, format):
    prefix = schema["definition"]["namespace"].replace(".", "/")
    operation_time = datetime.datetime.today().strftime('%Y%m%d%H%M%S')
    return "{prefix}/{operation}/{format}/{operation_time}".format(prefix=prefix,
                                                                   format=format,
                                                                   operation=operation, operation_time=operation_time)
