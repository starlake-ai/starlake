# Release notes

## 0.1.23

__New feature__:
- YML files are now renamed with the suffix .comet.yml
- Comet Schema is now published on SchemaStore. This allows Intellisense in VSCode & Intellij
- Assertions may now be executed as part of the Load and transform processes
- Shared Assertions UDF may be defined and stored in COMET_ROOT/metadata/assertions
- Views mays also be defined and shared in COMET_ROOT/metadata/views.
- Views are accessible in the load and transform processes.

__Breaking Changes__:
- N.A.

__Bugfix__:
- Use Spark Application Id for JobID information to make auditing easier

