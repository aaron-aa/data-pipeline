# Copyright (c) 2017 App Annie Inc. All rights reserved.
from distutils.core import setup

from pip.req import parse_requirements

with patch_core_lib() as a:
    setup(name=PKG_NAME,
          version="1.0.0",
          description='int data pipeline lib',
          author='App Annie',
          author_email='App Annie',
          url='https://github.com/aaron-aa/data-pipeline',
          package_data={'': ['*.json.gz']},
          packages=['extract', 'utils'],
          install_requires=[str(r.req) for r in parse_requirements(requirements, session='client')]
          )
