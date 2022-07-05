
# Release notes

# 0.3.20
__Bug Fix__:
- Keep retrocompatibility with scala 2.11

# 0.3.19
__New feature__:
- Handle Mixed XSD / YML ingestion & validation
- Support JSON / XML descriptions in XLS files
- Support arrays in XLS files

__Bug Fix__:
- Support file system sink options in autojob

# 0.3.18
__New feature__:
- Enhance XLS support for escaping char
- Support HTTP Stream Source
- Support XSD Validation
- Transform  jobs now report on the number of affected rows.

__Bug Fix__:
- Regression return value of an autojob

# 0.3.17
__New feature__:
- Support extra dsv options in conf file
- support any option stored in metadata.options as an option for the reader.
- Support VSCode  Development


# 0.3.16
__New feature__:
- Upgrade Kafka libraries
- Simplify removal of comments in autojobs SQL statements.

# 0.3.15

__New feature__:
- deprecate usage of schema, schemaRefs in domains and dataset in autojobs. Prefer the use of table and tableRefs

__Bug Fix__:
- fix regression on Merge mode without Timestamp option

# 0.3.14
__Bug Fix__:
- Xls2Yml - Get a correct sheet name based on the schema name field

# 0.3.13
__New feature__:
- Improve XLS support for long name
- Handle rate limit exceeded by setting COMET_GROUPED_MAX to avoid HTTP 429 on some cloud providers. 

# 0.3.12
__Bug Fix__: reorder transformation on attributes as follows:
- rename columns
  - run script fields
  - apply transformations (privacy: "sql: ...")
  - remove ignore fields
  - remove input filename column

# 0.3.11 
__Bug Fix__:
- Handle field relaxation when in Append Mode and table does not exist.

# 0.3.9 / 0.3.10 / 0.3.11
__Bug Fix__:
- Make fields in rejected table optional

# 0.3.8
__New feature__:
- Rollback on support for kafka.properties files. It is useless since we already have a server-options properties.

# 0.3.7
__New feature__:
- Improve XLS support for metadata

# 0.3.6
__New feature__:
- Autoload kafka.properties file from metadata directory.

# 0.3.5
__New feature__:
- Parallel copy of files when loading and archiving
- Support renaming of domains and schemas in XLS

# 0.3.3 / 0.3.4
- Fixing release process

# 0.3.2
__New feature__:
- import step can be limited to one or more domains

# 0.3.1
__New feature__:
- Update Kafka / BigQuery libraries
- Add new  [preset env vars](https://starlake-ai.github.io/starlake/docs/reference/environment#preset-variables)
- Allow renaming of domains and schemas

# 0.3.0
__New feature__:
- Vars in assertions are now substituted at load time
- Support SQL statement in privacy phase
- Support parameterized semantic types
- Add support for generic sink
- Allow use of custom deserializer on Kafka source

# 0.2.10
__New feature__:
- Drop Java 1.8 prerequisite for compilation  
- Support custom database name for Hive compatible metastore
- Support custom dataset name in BQ

# 0.2.9
__New feature__:
- Drop support for Spark 2.3.X
- Allow table renaming on write
- Any Spark supported input is now allowed
- Env vars in env.yml files

# 0.2.8
__New feature__:
- Generate DDL from YML files with support for BigQuery, Snowflake, Synapse and Postgres #51 / #56
- Improve XLS handling: Add support for presql / postsql, tags, primary and foreign keys #59
- Add optional application of row & column level security
- Databricks Support
- Signification reduction of memory consumption
- Support application.conf file in metadata folder (COMET_METADATA_FS and COMET_ROOT must still be passed as env variables)

__Bug Fix__:
- Include env var and option when running presql in ingestion mode #58

# 0.2.7
__New feature__:
- Support merging dataset with updated schema
- Support publishing to github packages
- Reduce number of dependencies
- Allow Audit sink name configuration from environment variable
- Dropped support for elasticsearch 6

__Bug Fix__:
- Support timestamps as long in XML & JSOn FIles

# 0.2.6
__New feature__:
- Support XML Schema inference
- Support the ability to reject the whole file on error
- Improve error reporting
- Support engine on task SQL (query pushdown to BigQuery)
- Support last(n) partition on merge
- Added new env var to control parititioning COMET_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE
- Added env var to control BigQuery materialization on pushdown queries COMET_SPARK_BIGQUERY_MATERIALIZATION_PROJECT, COMET_SPARK_BIGQUERY_MATERIALIZATION_DATASET (default to materalization)
- Added env var to control BigQuery read data format COMET_SPARK_BIGQUERY_READ_DATA_FORMAT (default to AVRO)
- When COMET_MERGE_OPTIMIZE_PARTITION_WRITE is set and dynamic partition is active, only write partition containing new records or records to be deleted or updated for BQ (handled by Spark by default for FS).
- Add VALIDATE_ON_LOAD (comet-validate-on-load) property to raise an exception if one of the domain/job YML file is invalid. default to false
- Add custom file extensions property in Domain import ```default-file-extensions``` and env var ```COMET_DEFAULT_FILE_EXTENSIONS```
__Bug Fix__:
- Loading empty files when the schema contains script fields
- Applying default value for an attribute when value in the input data is null
- Transformation job with BQ engine fails when no views block is defined
- XLS2YML : remove non-breaking spaces from Excel file cells to avoid parsing errors
- Fix merge using timestamp option
- Json ingestion fails with complex array of objects
- Remove duplicates on incoming when existingDF does not exist or is empty
- Parse Sink options correctly 
- Handle extreme cases where audit lock raise an exception on creation
- Handle files without extension in the landing zone
- Store audit log with batch priority on BigQuery

# 0.2.4 / 0.2.5
__Bug Fix__:
- Handle [Jackson bug](https://www.google.fr/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjo9qr3v4PxAhWNohQKHfh1CqoQFjAAegQIAhAD&url=https%3A%2F%2Fgithub.com%2FFasterXML%2Fjackson-module-scala%2Fissues%2F218&usg=AOvVaw02niMBgrqd-BWw7-e1YQfc)

# 0.2.3
__New feature__:
- Add ability to ignore some fields (only top level fields supported)
- **BREAKING CHANGE**: Handle multiple schemas during extraction. Update your `extract` configurations before migrating to this version.
- Improve InferSchemaJob
- Include primary keys & foreign keys in JDBC2Yml

__Bug Fix__:
- Handle rename in JSON / XML files
- Handle timestamp fields in JSON / XML files
- Do not partition rejected files
- Add COMET_CSV_OUTPUT_EXT env var to customize filename extension after ingestion when CSV is active.  

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
- Export all tables in JDBC2Yml generation
- Include table & column names when meeting unknown column type in JDBC source schema
- Better logging on forced conversion in JDBC2Yml
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
- Parameterize with Domain & Schema metadata in JDBC2Yml generation 
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
