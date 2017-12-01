#!/usr/bin/env python
# Copyright (c) 2017 App Annie Inc. All rights reserved.
# -*- coding:utf-8 -*-

from extract.interfaces import get, dummy_data
from utils.rawfs import RawDataFS
from metastore.manipulation import INSERT, DELETE, UPDATE, REFRESH


def update(data_name, data):
    delete(data_name, filters)
    add(interface_name, data)


def delete(data_name, data):
    interface = get(data_name, DELETE)
    fs = RawDataFS(DELETE, interface, data)
    fs.write()


def insert(data_name, data):
    interface = get(data_name, INSERT)
    fs = RawDataFS(INSERT, interface, data)
    fs.write()


if __name__ == '__main__':
    # please run in ipython
    from extract.python import insert
    from extract.interfaces import dummy_data
    insert("featured_story", dummy_data("featured_story", INSERT))
    delete("featured_story", dummy_data("featured_story", DELETE))
