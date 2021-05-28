---
sidebar_position: 1
title: Extract
---

This sample is available in the `samples/extract` directory 
First you need to set the JDBC connection to the database. Below an example on a H2 database:

```hocon
connections {
  h2-sample-db {
    format = "jdbc"
    options {
      url: "jdbc:h2:file:/my/h2db/path",
      driver: "org.h2.Driver"
    }
  }
}
```

Next create a file describing the schema you want to import. We provide below 2 examples.

### Example 1: 
Extract all objects from the `PUBLIC` schema.
```yaml
jdbc-schema:
  connection: "h2-sample-db" # Connection name as defined in the connections section of the application.conf file
  schema: "PUBLIC" # Database schema where tables are located
  tables:
    - name: "*" # Takes all tables
  tableTypes: # One or many of the types below
    - "TABLE"
    - "VIEW"
    - "SYSTEM TABLE"
    - "GLOBAL TEMPORARY"
    - "LOCAL TEMPORARY"
    - "ALIAS"
    - "SYNONYM"
  templateFile: "domain-template.yml" # Metadata to use for the generated YML file.

```

### Example 2: 
Extract only the selected tables from the `PUBLIC`schema and only the selected columns from the `votes` table.
```yaml
jdbc-schema:
  connection: "h2-sample-db" # Connection name as defined in the connections section of the application.conf file
  schema: "PUBLIC" # Database schema where tables are located
  tables:
    - name: "speakers"
    - name: "votes"
      columns:
        - speaker_id
        - id
        - rating
  tableTypes: # One or many of the types below
    - "TABLE"
  templateFile: "domain-template.yml" # Metadata to use for the generated YML file.
```


The `templateFile` section are used to set the default values for the metadata fields in the generated `load` file. 

