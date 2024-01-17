#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='starlake-airflow',
      version='0.0.11',
      description='Starlake Python Distribution For Airflow',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Stéphane Manciot',
      author_email='stephane.manciot@gmail.com',
      license='Apache 2.0',
#      url='https://www.python.org/starlake-airflow/',
      packages=find_packages(include=['ai', 'ai.*']),
#      python_requires='>=3.8',
)
