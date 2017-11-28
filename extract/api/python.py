#!/usr/bin/env python
# Copyright (c) 2017 App Annie Inc. All rights reserved.
# -*- coding:utf-8 -*-
import json
import hashlib
import os
import tempfile
import time


from avro import schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from itertools import groupby

from extract.schema import get
from utils.retry import retry
from utils.s3 import put
from utils.partition import signature, identifier, s3key_of_rawfile, data_partition_name


def update(schema_alias, filters, data):
    delete(schema_alias, filters)
    add(schema_alias, data)


def delete(schema_alias, filters):
    # country_code=? date=? ..., must exist in schema
    pass


def insert(schema_alias, data):
    """
    ss/story/v1/insert/20171127134325/dpcf9ae0dffac85337790b9e85f5025360/2017/11/31/apple-store/iphone/US.avro
    """
    schema_ = get(schema_alias)
    definition = schema_["definition"]

    identifier_ = identifier()
    signature_ = signature(data)
    namespace_ = schema_["definition"]["namespace"]

    pfields = schema_.get('partition_fields')
    grouped_data = groupby(data, key=lambda x: [x.get(y) for y in pfields]) if pfields else {"": data}

    for k, v in grouped_data:
        data_partition_name_ = data_partition_name(k)
        s3key = s3key_of_rawfile(namespace_, "insert", identifier_, signature_, data_partition_name_, "avro")
        with _TempFile(s3key) as f:
            avro_schema = schema.parse(json.dumps(definition))
            writer = DataFileWriter(f, DatumWriter(), avro_schema, codec="deflate")
            for r in v:
                writer.append(r)
            writer.close()

            # 1. ***put file***
            retry(put, ("aaron-s3-poc", s3key, open(f.name, "rb")))
            # success_key = "/".join(("/".join(key.split("/")[:-1]), "_SUCCESS"))
            # retry(put, ("aaron-s3-poc", success_key, ""))

            # 2. *** update meta ***
            # 3. *** trigger event***


class _TempFile(object):
    def __init__(self, name):
        self.path = os.path.join(tempfile.gettempdir(), name.replace("/", "-"))

    def __enter__(self):
        return open(self.path, "wb")

    def __exit__(self, *args):
        try:
            os.remove(self.path)
        except:
            pass


if __name__ == '__main__':
    from extract.api.demodata import DATA
    insert("story", DATA)
