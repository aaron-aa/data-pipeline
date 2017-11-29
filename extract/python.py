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
from avro.io import DatumReader, DatumWriter, validate
from copy import deepcopy
from itertools import groupby


from extract.interfaces import get, dummy_data
from utils.retry import retry
from utils.s3 import put
from utils.partition import signature, identifier, s3key_of_rawfile, data_partition_name


def update(interface_name, filters, data):
    delete(interface_name, filters)
    add(interface_name, data)


def delete(interface_name, filters_list):
    # country_code=? date=? ..., must exist in schema
    _operation_template("delete", interface_name, filters_list)


def insert(interface_name, data):
    """
    raw/ss/featured_story/v1/insert/20171128164025-signature=dpea77f3f880e95056bae9282519e9f801-date=2017-11-31-market_code=apple-store-device_code=iphone-country_code=JP.avro
    """
    _operation_template("insert", interface_name, data)


def _operation_template(operation, interface_name, data_list):
    interface = get(interface_name, operation)
    schema_ = interface["schema"]

    identifier_ = identifier()
    signature_ = signature(data_list)
    namespace_ = schema_["namespace"]

    # required
    pfield_names = interface['partition_fields']
    grouped_data = groupby(data_list, key=lambda x: [x.get(y) for y in pfield_names])

    for k, v in grouped_data:
        partition_fields = zip(pfield_names, k)
        s3key = s3key_of_rawfile(namespace_, operation, identifier_, partition_fields, signature_)
        _write_s3(s3key, schema_, v)


def _write_s3(s3key, schema_, data):
    with _TempFile(s3key) as f:
        avro_schema = schema.parse(json.dumps(schema_))
        writer = DataFileWriter(f, DatumWriter(), avro_schema, codec="deflate")
        for r in data:
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
    # please run in ipython
    from extract.python import insert
    from extract.interfaces import dummy_data
    insert("featured_story", dummy_data("featured_story"))
