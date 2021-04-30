![Build Status](https://github.com/ebiznext/comet-data-pipeline/workflows/Build/badge.svg)
![Snapshot Status](https://github.com/ebiznext/comet-data-pipeline/workflows/Snapshot/badge.svg)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![codecov](https://codecov.io/gh/ebiznext/comet-data-pipeline/branch/master/graph/badge.svg)](https://codecov.io/gh/ebiznext/comet-data-pipeline)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/f79729e67cce45aba81e1950b91ef8eb)](https://www.codacy.com/gh/ebiznext/comet-data-pipeline/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ebiznext/comet-data-pipeline&amp;utm_campaign=Badge_Grade)
[![Documentation](https://img.shields.io/badge/docs-passing-green.svg)](https://ebiznext.github.io/comet-data-pipeline/)
[![Maven Central Comet Spark 3](https://maven-badges.herokuapp.com/maven-central/com.ebiznext/comet-spark3_2.12/badge.svg)](https://mvnrepository.com/artifact/com.ebiznext/comet-spark3_2.12)
[![Join the chat at https://gitter.im/comet-data-pipeline/community](https://badges.gitter.im/comet-data-pipeline/community.svg)](https://gitter.im/comet-data-pipeline/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# About Comet Data Pipeline

Complete documentation available [here](https://ebiznext.github.io/comet-data-pipeline/)

## Introduction

The purpose of this project is to efficiently ingest various data
sources in different formats and make them available for analytics.
Usually, ingestion is done by writing hand made custom parsers that
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

![Complete Comet Data pipeline]( docs/static/img/guide/cdp-howitworks.png "Complete Comet Data pipeline")


1. Landing Area : Files are first stored in the local file system
2. Staging Area : Files associated with a schema are imported into the datalake
3. Working Area : Staged Files are parsed against their schema and records are rejected or accepted and made available in parquet/orc/... files as Hive Tables.
4. Business Area : Tables in the working area may be joined to provide a hoslictic view of the data through the definition of AutoJob.
5. Data visualization : parquet/orc/... tables may be exposed in datawarehouses or elasticsearch indexes


