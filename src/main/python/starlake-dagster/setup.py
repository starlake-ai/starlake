#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='starlake-dagster',
      version='0.1.0',
      description='Starlake Python Distribution For Dagster',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Stéphane Manciot',
      author_email='stephane.manciot@gmail.com',
      license='Apache 2.0',
#      url='https://github.com/starlake-ai/starlake/tree/master/src/main/python/starlake-dagster',
      packages=find_packages(include=['ai', 'ai.*']),
      install_requires=['starlake-orchestration>=0.1.0'],
      extras_require={
        "dagster": [], #["dagster"],
        "shell": ["dagster-shell"],
        "gcp": [], #["dagster-gcp"],
        "aws": [],
        "azure": [],
      },
#      python_requires='>=3.8',
)
