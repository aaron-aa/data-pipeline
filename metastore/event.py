from utils.retry import retry
from utils.s3 import put
from utils.siganature import datasign

"""
http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
For example:
{
   "bucket": "prod.appannie.int.data.pipeline.metastore",
   "prefix": "events/",
   "postfix": "event",
   "pattern":
    "events/{?P<event_type>.+}/{?P<namespace>.+}/{?P<manipulation>.+}/{?P<identifier>.+}-hashcode.event"
}

eventtype:
TRANSFORM
LOAD
OPERATOR

manipulation:
INSERT
DELETE
UPDATE
REFRESH

content:
data path
s3a://aaron-s3-poc/
"""


class S3(object):
    BUCKET = "aaron-s3-poc"
    ROOT = "events"

    @classmethod
    def send(cls, event_type, namespace, manipulation, identifier, data_path, data_format):
        # write s3 file
        s3key = "{root}/{event_type}/{namespace}/{manipulation}/{identifier}-{hashcode}.event".format(
            root=cls.ROOT,
            event_type=event_type,
            namespace=namespace,
            manipulation=manipulation,
            identifier=identifier,
            hashcode=datasign(data_path)
        )
        retry(put, (cls.BUCKET, s3key, "path,format\n{},{}".format(data_path, data_format)))

    @classmethod
    def receive(cls, spark, event_type, namespace, manipulation, identifier):
        s3key = "s3a://{bucket}/{root}/{event_type}/{namespace}/{manipulation}/{identifier}*".format(
            bucket=cls.BUCKET,
            root=cls.ROOT,
            event_type=event_type,
            namespace=namespace,
            manipulation=manipulation,
            identifier=identifier)
        return spark.read.option("header", "true").option("sep", ",").csv(s3key)
