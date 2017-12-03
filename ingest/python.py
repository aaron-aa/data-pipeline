#!/usr/bin/env python
# Copyright (c) 2017 App Annie Inc. All rights reserved.
# -*- coding:utf-8 -*-

from ingest.interfaces import get, dummy_data
from utils.rawfs import RawDataFS
from metastore.manipulation import INSERT, DELETE, UPDATE, REFRESH


def refresh(data_name, data):
    interface = get(data_name, REFRESH)
    fs = RawDataFS(REFRESH, interface, data)
    fs.write()


def update(data_name, data):
    interface = get(data_name, UPDATE)
    fs = RawDataFS(UPDATE, interface, data)
    fs.write()


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
    from ingest.python import insert, update, delete
    from ingest.interfaces import dummy_data
    from metastore.manipulation import INSERT, DELETE, UPDATE, REFRESH
    insert("featured_story", dummy_data("featured_story", INSERT))
    update("featured_story", dummy_data("featured_story", UPDATE))
    delete("featured_story", dummy_data("featured_story", DELETE))
