![Build Status](https://github.com/starlake-ai/starlake/workflows/Build/badge.svg)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![codecov](https://codecov.io/gh/starlake-ai/starlake/branch/master/graph/badge.svg)](https://codecov.io/gh/starlake-ai/starlake)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/569178d6936842808702e72c30d74643)](https://www.codacy.com/gh/starlake-ai/starlake/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=starlake-ai/starlake&amp;utm_campaign=Badge_Grade)
[![Documentation](https://img.shields.io/badge/docs-passing-green.svg)](https://starlake-ai.github.io/starlake/)
[![Maven Central Starlake Spark 3](https://maven-badges.herokuapp.com/maven-central/ai.starlake/starlake-spark3_2.12/badge.svg?gav=true)](https://maven-badges.herokuapp.com/maven-central/ai.starlake/starlake-spark3_2.12)
[![discord](https://img.shields.io/discord/833336395430625310.svg?logo=discord&logoColor=fff&label=Discord&color=7389d8)](https://discord.com/channels/833336395430625310/908709208025858079)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# About Starlake

Complete documentation available [here](https://starlake-ai.github.io/starlake/index.html)

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


The main advantages of the Starlake Data Pipeline project are :

* Eliminates manual coding for data ingestion
* Assign metadata to each dataset
* Expose data ingestion metrics and history
* Transform text files to strongly typed records
* Support semantic types
* Force privacy on specific fields (RGPD)
* very, very simple piece of software to administer

## How it works

Starlake Data Pipeline automates the loading and parsing of files and
their ingestion into a Hadoop Datalake where datasets become
available as Hive tables.

![Complete Starlake Data Pipeline]( docs/static/img/guide/animated-elt.gif "Complete Starlake Data Pipeline")


1. Landing Area : Files are first stored in the local file system
2. Staging Area : Files associated with a schema are imported into the datalake
3. Working Area : Staged Files are parsed against their schema and records are rejected or accepted and made available in parquet/orc/... files as Hive Tables.
4. Business Area : Tables in the working area may be joined to provide a hoslictic view of the data through the definition of AutoJob.
5. Data visualization : parquet/orc/... tables may be exposed in datawarehouses or elasticsearch indexes


