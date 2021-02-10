# Release notes

## 0.1.34
__New feature__:
- Allow sink options to be defined in YML instead of Spark Submit

## 0.1.29
__Bugfix__:
- Support correctly merge mode in BQ

## 0.1.27
__New feature__:
- Kafka Support improved

## 0.1.26
__New feature__:
- Optionally sink to file using property sink-to-file = ${?COMET_SINK_TO_FILE}

__Bugfix__:
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

__Bugfix__:
- Use Spark Application Id for JobID information to make auditing easier

