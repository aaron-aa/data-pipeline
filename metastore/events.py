class S3Event(object):
    def __init__(self, pattern, prefix=None, postfix=None):
        """
        http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
        For example:
        {
           "bucket": "prod.appannie.int.data.pipeline.metastore",
           "prefix": "events/",
           "postfix": "EVENT",
           "pattern": "events/{?P<eventtype>.+}/{?P<namespace>.+}/{?P<manipulation>.+}/{?P<timestamp>.+}/EVENT"
        }

        eventtype:
        TRANSFORM
        LOAD
        OPERATOR

        manipulation:
        INSERT
        DELETE
        UPDATE

        content:
        data path
        """
        pass

    def send(self):
        # write s3 file
        pass
