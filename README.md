[![Build Status](https://travis-ci.com/ebiznext/comet-data-pipeline.svg?branch=master)](https://travis-ci.com/ebiznext/comet-data-pipeline)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![codecov](https://codecov.io/gh/ebiznext/comet-data-pipeline/branch/master/graph/badge.svg)](https://codecov.io/gh/ebiznext/comet-data-pipeline)
[![Documentation](https://readthedocs.org/projects/comet-app/badge/?version=latest)](https://comet-app.readthedocs.io/)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.ebiznext/comet/badge.svg)](https://mvnrepository.com/artifact/com.ebiznext/comet)

# About Comet Data Pipeline

Complete documentation available [here](https://comet-app.readthedocs.io/)

## Introduction

The purpose of this project is to efficiently ingest various data
sources in different formats and make them available for analytics.
Usualluy, ingestion is done by writing hand made custom parsers that
transform input files into datasets of records.

This project aims at automating this parsing task by making data
ingestion purely declarative.

The workflow below is a typical use case :

* Export your data as a set of DSV (Delimiter-separated values) or JSON files
* Define each DSV/JSON file with a schema using YAML syntax
* Configure the ingestion process
* Start watching your data being available as Hive Tables in your  datalake


The main advantages of the Comet Data Pipeline project are :

* Eliminates manual coding for data ingestion
* Assign metadata to each dataset
* Expose data ingestion metrics and history
* Transform text files to strongly typed records
* Support semantic types
* Force privacy on specific fields (RGPD)
* very, very simple piece of software to administer

## How it works

Comet Data Pipeline automates the loading and parsing of files and
their ingestion into a Hadoop Datalake where datasets become
available as Hive tables.

![Complete Comet Data pipeline]( docs/user/assets/cdp-howitworks.png "Complete Comet Data pipeline")


1. Landing Area : Files are first stored in the local file system
2. Staging Area : Files associated with a schema are imported into the datalake
3. Working Area : Staged Files are parsed against their schema and records are rejected or accepted and made available in parquet/orc/... files as Hive Tables.
4. Business Area : Tables in the working area may be joined to provide a hoslictic view of the data through the definition of AutoJob.
5. Data visualization : parquet/orc/... tables may be exposed in datawarehouses or elasticsearch indexes


