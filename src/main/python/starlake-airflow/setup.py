#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='starlake-airflow',
      version='0.3.1.2',
      description='Starlake Python Distribution For Airflow',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Stéphane Manciot',
      author_email='stephane.manciot@gmail.com',
      license='Apache 2.0',
#      url='https://github.com/starlake-ai/starlake/tree/master/src/main/python/starlake-airflow',
      packages=find_packages(include=['ai', 'ai.*']),
      install_requires=['starlake-orchestration>=0.3.2'],
      extras_require={
        "airflow": ["airflow>=2.10.0"],
        "shell": [],
        "gcp": [], #["apache-airflow-providers-google>=10.0.7"]
        "aws": [],
        "azure": [],
      },
#      python_requires='>=3.8',
)
