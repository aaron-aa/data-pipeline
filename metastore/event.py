from utils.retry import retry
from utils.s3 import put
from utils.siganature import datasign
from utils.avro_ import create, write
from utils.file_ import Temp
from metastore.manipulation import INSERT, DELETE, UPDATE, REFRESH
from metastore.event_type import TRANSFORM, LOAD, OPERATOR

"""
http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
For example:
{
   "bucket": "prod.appannie.int.data.pipeline.metastore",
   "prefix": "events/",
   "postfix": "event",
   "pattern":
    "events/{?P<event_type>.+}/{?P<namespace>.+}/{?P<manipulation>.+}/{?P<identifier>.+}-hashcode.avro"
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
    SCHEMA = create({
        "namespace": "data-pipeline.event.v1",
        "type": "record",
        "name": "MetaEvent",
        "alias": ["meta_event"],
        "fields": [
            {
                "name": "type",
                "type": {
                    "type": "enum",
                    "name": "e_type",
                    "symbols": [TRANSFORM, LOAD, OPERATOR]
                }
            },
            {"name": "namespace", "type": "string"},
            {
                "name": "manipulation",
                "type": {
                    "type": "enum",
                    "name": "e_manipulation",
                    "symbols": [INSERT, DELETE, UPDATE, REFRESH]
                }
            },
            {"name": "identifier", "type": "long"},
            {"name": "path", "type": "string"},
            {
                "name": "format",
                "type": {
                    "type": "enum",
                    "name": "e_format",
                    "symbols": ["avro", "parquet"]
                }
            }
        ]
    })

    @classmethod
    def send(cls, event_type, namespace, manipulation, identifier, data_path, data_format):
        # write s3 file
        s3key = "{root}/{event_type}/{namespace}/{manipulation}/{identifier}-{hashcode}.avro".format(
            root=cls.ROOT,
            event_type=event_type,
            namespace=namespace,
            manipulation=manipulation,
            identifier=identifier,
            hashcode=datasign(data_path)
        )
        with Temp(s3key) as f:
            content = [{
                "type": event_type,
                "namespace": namespace,
                "manipulation": manipulation,
                "identifier": identifier,
                "path": data_path,
                "format": data_format
            }]
            write(cls.SCHEMA, f, content)
            retry(put, (cls.BUCKET, s3key, open(f.name, "rb")))

    @classmethod
    def receive(cls, spark, event_type, namespace, manipulation, identifier):
        s3key = "s3a://{bucket}/{root}/{event_type}/{namespace}/{manipulation}/{identifier}*".format(
            bucket=cls.BUCKET,
            root=cls.ROOT,
            event_type=event_type,
            namespace=namespace,
            manipulation=manipulation,
            identifier=identifier)
        return spark.read.format("com.databricks.spark.avro").load(s3key)
