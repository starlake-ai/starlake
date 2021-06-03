# Release notes

# 0.2.3
- Add ability to ignore some fields (only top level fields supported)
- Improve InferSchmaJob
- Include primary keys & foreign keys in DDL2Yml

## 0.2.2
__New feature__:
- Use the same variable for Lock timeout
- Improve logging when locking file fails
- File sink while still the default is now controlled by the sink tag in the YAML file. The option sink-to-file is removed and used for testing purpose only.
- Allow custom topic name for comet_offsets
- Add ability to coalesce(int) to kafka offloading feature
- Attributes may now be declared as primary and or foreign keys even though no check is made.  
- Export schema and  relations(PK / FK) as dot (graphviz) files.
- Support saving comet offsets to filesystem instead of kafka using the new setting comet-offsets-mode = "STREAM"

__Bug Fix__:
- Invalid YAML files produce now an error at startup instead of displaying a warning.

## 0.2.1
- Version skipped

## 0.2.0
__New feature__:
- Export all tables in DDL2YML generation
- Include table & column names when meeting unknown column type in JDBC source schema
- Better logging on forced conversion in DDL2YML
- Compute Hive Statistics on Table & Partitions
- DataGrip support with implementation of substitution for ${} in addition to {{}}
- Improve logging
- Add column type during for database extraction
- The name attribute inside a job file should reflect the filename. This attribute will soon be deprecated
- Allow Templating on jobs. Useful to generate Airflow / Oozie Dags from job.comet.yml/job.sql code
- Switch from readthedocs to docusaurus
- Add local and bigquery samples
- Custom var pattern through sql-pattern-parameter in reference.conf

__Bug Fix__:
- Avoid computing statistics on struct fields
- Make database-extractor optional in application.conf


## 0.1.36
__New feature__:
- Parameterize with Domain & Schema metadata in DDL2YML generation 
__Bug Fix__:

## 0.1.35
__New feature__:
- Auto compile with scala 2.11 for Spark 2 and with scala 2.12 for Spark 3. [[457]](https://github.com/ebiznext/comet-data-pipeline/pull/457)
- Performance optimization when using Privacy Rules. [[459]](https://github.com/ebiznext/comet-data-pipeline/pull/459)
- Rejected area and audit logs support can have their own write format (default-rejected-write-format and default-audit-write-format properties)
- Deep JSON & XML files are now validated against the schema
- Privacy is applied on deep JSON & XML inputs [[461]](https://github.com/ebiznext/comet-data-pipeline/pull/461)
- Domains & Jobs may be defined in subdirectories allowing better metatdata files organization [[462]](https://github.com/ebiznext/comet-data-pipeline/pull/462)
- Substitute variables through CLI & env files in views, assertions, presql, main sql and post sql requests [[462]](https://github.com/ebiznext/comet-data-pipeline/pull/462)
- Semantic type Date supports dates with _MMM_ month representation [[463]](https://github.com/ebiznext/comet-data-pipeline/pull/463)
- Split reference.conf into multiple files. [[460]](https://github.com/ebiznext/comet-data-pipeline/pull/460)
- Support kafka Source & Sink through Spark Streaming [[460]](https://github.com/ebiznext/comet-data-pipeline/pull/460)
- Add an alternative way for applying privacy on XML files.[[466]](https://github.com/ebiznext/comet-data-pipeline/pull/466)
- Generate Excel files from YML files
- Generate YML file from Database Schema

__Bug Fix__:
- Make Jackson lib provided. [[457]](https://github.com/ebiznext/comet-data-pipeline/pull/457)
- Support Spark 2.3. by not using Dataframe.isEmpty [[457]](https://github.com/ebiznext/comet-data-pipeline/pull/457)
- _comet_input_file_name_ missing when ingesting Position files [[466]](https://github.com/ebiznext/comet-data-pipeline/pull/466)
- Apply postsql queries on the accepted DataFrame [[466]](https://github.com/ebiznext/comet-data-pipeline/pull/466)
- Check that scripted fields are defined at the end of the schema in the YML file [#384]

## 0.1.34
__New feature__:
- Allow sink options to be defined in YML instead of Spark Submit. [#450] [#454]

__Bug Fix__:
- Parse dates with yyyyMM format correctly [#451]
- Fix error when saving a csv with an empty DataFrame [#451]
- Keep column description in BQ tables when using Overwrite mode [#453]

## 0.1.29
__Bug Fix__:
- Support correctly merge mode in BQ [#449]
- Fix for sinking XML to BQ [#448]

## 0.1.27
__New feature__:
- Kafka Support improved

## 0.1.26
__New feature__:
- Optionally sink to file using property sink-to-file = ${?COMET_SINK_TO_FILE}

__Bug Fix__:
- Sink name was ignored and always considered as None

## 0.1.23
__New feature__:
- YML files are now renamed with the suffix .comet.yml
- Comet Schema is now published on SchemaStore. This allows Intellisense in VSCode & Intellij
- Assertions may now be executed as part of the Load and transform processes
- Shared Assertions UDF may be defined and stored in COMET_ROOT/metadata/assertions
- Views mays also be defined and shared in COMET_ROOT/metadata/views.
- Views are accessible in the load and transform processes.
- Domain may be now prefixed by the "load" tag. Defining a domain without the "load" tag is now deprecated
- AutoJob may be now prefixed by the "transform" tag. Defining a autojob without the "transform" tag is now deprecated

__Breaking Changes__:
- N.A.

__Bug Fix__:
- Use Spark Application Id for JobID information to make auditing easier

## 0.1.22
__New feature__:
- Expose a REST API to generate a Yaml Schema from an Excel file. [#387]
- Support ingesting multiline complex JSON. [#391]
- Support nested fields when generating schema for BigQuery tables. [#391]
- Enhancements on Spark to BigQuery schema. [#395]
- Support merging a part of a BigQuery Table, rather than all the Table. [#397]
- Enable setting BigQuery intermediate format when sinking using ${?COMET_INTERMEDIATE_BQ_FORMAT}. [#398] [#400]
- Enhancement on Merging mode: do not depend on parquet files when using BigQuery tables.

__Dependencies__:
- Update sbt to 1.4.4 [#385]
- Update scopt to 4.0.0 [#390]
