#!/usr/bin/env python
# Copyright (c) 2017 App Annie Inc. All rights reserved.
# -*- coding:utf-8 -*-
from importlib import import_module


def get(alias, operation):
    return import_module('ingest.interfaces.{}'.format(alias)).INTERFACE["operations"][operation]


def dummy_data(alias, operation):
    return getattr(import_module('ingest.interfaces.{}'.format(alias)),
                   "{}_DUMMY_DATA".format(operation.upper()))
