#!/usr/bin/env python
# Copyright (c) 2017 App Annie Inc. All rights reserved.
# -*- coding:utf-8 -*-
from importlib import import_module


def get(alias, operation):
    return import_module('extract.interfaces.{}'.format(alias)).INTERFACE["operations"][operation]


def dummy_data(alias):
    return import_module('extract.interfaces.{}'.format(alias)).DUMMY_DATA
