---
sidebar_position: 1
---

# Extract

This step is optional and useful only if you intend to extract data from a SQL Database into
a set of files before ingesting it into a datalake or data warehouse.

To extract the tables into DSV files, create a YAML specification file
that describe the tables and columns you are willing to extract using the following syntax:

````yaml
jdbc-schema:
  connection: "test-h2" # Connection name as defined in the connections section of the application.conf file
  catalog: "business" # Optional catalog name in the target database
  schema: "public" # Database schema where tables are located
  tables:
    - name: "user"
      columns: # optional list of columns, if not present all columns should be exported.
        - id
        - email
    - name: product # All columns should be exported
    - name: "*" # Ignore any other table spec. Just export all tables
  table-types: # One or many of the types below
    - "TABLE"
    - "VIEW"
    - "SYSTEM TABLE"
    - "GLOBAL TEMPORARY"
    - "LOCAL TEMPORARY"
    - "ALIAS"
    - "SYNONYM"
  template-file: "/my-templates/domain-template.yml" # Metadata to use for the generated YML file.
````

This will generate a YML file with the metadata file

Then you can :ref:`howto_extract` data.
Once data are extracted you can proceed to the :ref:`load` step.
