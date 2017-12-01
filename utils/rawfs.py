import hashlib
import json

from copy import deepcopy
from itertools import groupby

from metastore.event import S3
from metastore.event_type import TRANSFORM

from utils.retry import retry
from utils.s3 import put
from utils.file_ import Temp
from utils.avro_ import write
from utils.siganature import datasign
from utils.identifier import ID


class RawDataFS(object):
    BUCKET = "aaron-s3-poc"
    ROOT = "raw"
    FORMAT = 'avro'

    def __init__(self, manipulation, interface, data):
        self.manipulation = manipulation
        self.identifier = ID()

        self.interface = interface
        self.schema = interface["schema"]
        self.namespace = self.schema["namespace"]

        self.data = data
        self.signature = datasign(data)

    def write(self):
        self._template()

    def _template(self):
        pfield_names = self.interface['partition_fields']
        grouped_data = groupby(self.data, key=lambda x: [x.get(y) for y in pfield_names])

        key_prefix = self._key_prefix()
        for k, v in grouped_data:
            partition_fields = zip(pfield_names, k)
            s3key = key_prefix + self._key_postfix(partition_fields)
            # 1. ***put file***
            with Temp(s3key) as f:
                write(self.schema, f, v)
                retry(put, (self.BUCKET, s3key, open(f.name, "rb")))
        # 2. *** update meta ***
        # 3. *** trigger event***
        S3.send(TRANSFORM, self.namespace, self.manipulation, self.identifier, self._key_for_event(), self.FORMAT)

    def _key_postfix(self, partition_fields):
        """
        -signature=dpea77f3f880e95056bae9282519e9f801-data_granularity=daily-date=2017-11-31
        -market_code=apple-store-device_code=iphone-country_code=JP.avro
        """
        return "{signature_sep}{signature}{signature_sep}{partition_path}.{format}".format(
            signature="signature={}".format(self.signature),
            signature_sep="-" if self.signature else "",
            partition_path=self._partition_path(partition_fields),
            format=self.FORMAT
        )

    def _key_prefix(self):
        """
        raw/ss.featured_story.v1/insert/20171128215207
        """
        return "{root}/{namespace}/{manipulation}/{identifier}".format(
            root=self.ROOT,
            manipulation=self.manipulation,
            namespace=self.namespace,
            identifier=self.identifier
        )

    def _key_for_event(self):
        """
        s3://aaron-s3-poc/raw/ss.featured_story.v1/insert/20171128215207*
        """
        return "{bucket}/{prefix}*".format(
            bucket=self.BUCKET,
            prefix=self._key_prefix()
        )

    def _partition_path(self, partition_fields):
        return "-".join(map(lambda (x, y): "{}={}".format(x, y), partition_fields))
