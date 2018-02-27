#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Exdemon REST API
"""

from setuptools import setup, find_packages

setup(name='exdemon-api',
      version='0.2.0',
      description='Exdemon REST API',
      author='CERN',
      author_email='j.cordero@cern.ch',
      license='GPLv3',
      maintainer='Jose Andres Cordero',
      maintainer_email='j.cordero@cern.ch',
      url='https://github.com/cerndb/exdemon',
      packages=find_packages(),
      scripts=['exdemon-api'],
      test_suite="",
      install_requires=[
          'flask',
          'configparser',
          'flask-sqlalchemy',
          'psycopg2',
		  'flask-marshmallow',
		  'marshmallow-sqlalchemy'
          ],
     )