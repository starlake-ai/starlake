#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='starlake-orchestration',
      version='0.2.6',
      description='Starlake Python Distribution For orchestration',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Stéphane Manciot',
      author_email='stephane.manciot@gmail.com',
      license='Apache 2.0',
#      url='https://github.com/starlake-ai/starlake/tree/master/src/main/python/starlake-orchestration',
      packages=find_packages(include=['ai', 'ai.*']),
      extras_require={
        "airflow": ["starlake-airflow>=0.2.6"],
        "dagster": ["starlake-dagster>=0.2.5"],
        "snowflake": ["starlake-snowflake>=0.1.4"],
        "shell": [],
        "gcp": [],
        "aws": [],
        "azure": [],
      },
#      python_requires='>=3.8',
)
