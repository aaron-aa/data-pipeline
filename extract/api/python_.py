#!/usr/bin/env python
# Copyright (c) 2017 App Annie Inc. All rights reserved.
# -*- coding:utf-8 -*-
import boto3
import json
import hashlib
import os
import tempfile
import time


from avro import schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from extract.schema import get
from utils.retry import retry
from utils.s3 import put
from utils.partition import root


def update(schema_alias, filters, data):
    delete(schema_alias, filters)
    add(schema_alias, data)


def delete(schema_alias, filters):
    # country_code=? date=? ..., must exist in schema
    pass


def insert(schema_alias, data):
    """
    """
    schema_ = get(schema_alias)
    definition = schema_["definition"]
    identifier = _identifier(data)
    with _TempFile(data, identifier) as f:
        avro_schema = schema.parse(json.dumps(definition))
        writer = DataFileWriter(f, DatumWriter(), avro_schema, codec="deflate")
        for r in data:
            writer.append(r)
        writer.close()

        # 1. ***put file***
        # 1856 ss/story/v1/insert/20171126170538-06a4c3e299314a56dcb2a01f05083c6c.avro
        key = root(schema_, "insert", identifier, "avro")
        retry(put, ("aaron-s3-poc", key, open(f.name, "rb")))
        # success_key = "/".join(("/".join(key.split("/")[:-1]), "_SUCCESS"))
        # retry(put, ("aaron-s3-poc", success_key, ""))

        # 2. *** update meta ***
        # 3. *** trigger event***


def _identifier(data):
    md5 = hashlib.md5(json.dumps(data)).hexdigest()
    return "datapipeline-{}".format(md5)


class _TempFile(object):
    def __init__(self, data, identifier):
        self.path = os.path.join(tempfile.gettempdir(), identifier)

    def __enter__(self):
        return open(self.path, "wb")

    def __exit__(self, *args):
        try:
            os.remove(self.path)
        except:
            pass


if __name__ == '__main__':
    data = [{
        "data_type": "dimension",
        "data_granularity": "daily",
        "country_code": u"US",
        "year": 2017,
        "month": 11,
        "day": 31,
        "id": 1234567,
        "url": u"https://itunes.apple.com/cn/story/id1298971915?l=en&isWebExpV2=true&dataOnly=true",
        "position": 0,
        "creative_urls": [
            u"https://is5-ssl.mzstatic.com/image/thumb/Features118/v4/04/30/34/043034e6-b0c5-15bc-5366-e6498d896b21/source/1200x600bf.jpg"
        ],
        "app_ids": [438475005],
        "label": u"APP\nOF THE\nDAY",
        "head": u"TextGrabber 6 Real-Time OCR",
        "description": u"ABBYY TextGrabber easily and quickly digitizes fragments of printed text",
        "display_style": {
            "displayStyle": u"Branded",
            "cardDisplayStyle": u"AppOfTheDay",
            "displaySubStyle": u"AppOfDay"
        },
        "raw_data": ""
    }]
    insert("story", data)
