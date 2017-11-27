#!/usr/bin/env python
# Copyright (c) 2017 App Annie Inc. All rights reserved.
# -*- coding:utf-8 -*-
from importlib import import_module


def get(alias):
    return import_module('extract.schema.{}'.format(alias)).INTERFACE
