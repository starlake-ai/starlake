---
sidebar_position: 2
title: Load
---

This section describes how to import text files (eq. json / CSV) files into your Data Factory.

## Load to Parquet
Files will be ingested and stored in parquet format in the `$COMET_DATASETS/sales/customers` and `$COMET_DATASETS/sales/orders` files.

````yaml
load:
    name: "sales"
    directory: "__COMET_TEST_ROOT__/incoming/sales"
    metadata:
      mode: "FILE"
      format: "DSV"
      withHeader: true
      quote: "\""
      escape: "\\"
      write: "APPEND"
    schemas:
      - name: "customers"
        pattern: "customers-.*.psv"
        metadata:
          separator: "|"
        attributes:
          - name: "id"
            type: "customerid"
            required: true
          - name: "signup"
            type: "datetime"
            required: false
          - name: "contact"
            type: "email"
            required: false
          - name: "birthdate"
            type: "date"
            required: false
          - name: "name1"
            type: "string"
            required: false
            rename: "firstname"
          - name: "name2"
            type: "string"
            required: false
            rename: "lastname"
      - name: "orders"
        pattern: "orders-.*.csv"
        merge:
          key:
            - "id"
          delete: "customer_id is null"
        metadata:
          separator: ","
        attributes:
          - name: "order_id"
            type: "string"
            required: true
            rename: "id"
          - name: "customer_id"
            type: "customerid"
            required: false
          - name: "amount"
            type: "decimal"
            required: false
          - name: "seller_id"
            type: "string"
            required: false
````

## Load to BigQuery
Based on the [Load to parquet](#load-to-parquet) example, the only thing we add is the /metadata/sink section
Files will be stored in the `customers` and `orders` BigQuery tables under the `sales` BigQuery dataset

````yaml
load:
    name: "sales"
    directory: "/incoming/sales"
    metadata:
      mode: "FILE"
      format: "DSV"
      withHeader: true
      quote: "\""
      escape: "\\"
      write: "APPEND"
      sink:
        type: BQ
    schemas:
      - name: "customers"
        pattern: "customers-.*.psv"
        metadata:
          separator: "|"
        attributes:
          - name: "id"
            type: "customerid"
            required: true
          - name: "signup"
            type: "datetime"
            required: false
          - name: "contact"
            type: "email"
            required: false
          - name: "birthdate"
            type: "date"
            required: false
          - name: "name1"
            type: "string"
            required: false
            rename: "firstname"
          - name: "name2"
            type: "string"
            required: false
            rename: "lastname"
      - name: "orders"
        pattern: "orders-.*.csv"
        merge:
          key:
            - "id"
          delete: "customer_id is null"
        metadata:
          separator: ","
        attributes:
          - name: "order_id"
            type: "string"
            required: true
            rename: "id"
          - name: "customer_id"
            type: "customerid"
            required: false
          - name: "amount"
            type: "decimal"
            required: false
          - name: "seller_id"
            type: "string"
            required: false
````

## Load to SQL Database

Based on the [Load to parquet](#load-to-parquet) example, we need to

1. Add  the /metadata/sink section

````yaml
load:
    name: "hr"
    directory: "/incoming/hr"
    metadata:
      mode: "FILE"
      format: "JSON"
      sink:
        type: JDBC
        connection: my_connection
        partitions: 10
        batchSize: 1000
    schemas:
      - name: "sellers"
        pattern: "sellers-.*.json"
        metadata:
          array: true
          format: "SIMPLE_JSON"
          write: "APPEND"
        attributes:
          - name: "id"
            type: "string"
            required: true
          - name: "seller_email"
            type: "email"
            required: true
          - name: "location_id"
            type: "long"
            required: true
      - name: "locations"
        pattern: "locations-.*.json"
        metadata:
          format: "JSON"
          write: "OVERWRITE"
        attributes:
          - name: "id"
            type: "string"
            required: true
          - name: "address"
            type: "struct"
            required: true
            attributes:
              - name: "city"
                type: "string"
                required: true
                metricType: "discrete"
              - name: "stores"
                type: "string"
                array: true
                required: false
              - name: "country"
                type: "string"
                required: true
                metricType: "discrete"
````

2. Add to the jdbc section a connection with the name specified in the /medata/sink/connection property

````javascript
jdbc = {
  "my_connection": {
    uri = "jdbc:postgresql://127.0.0.1:5403/mydb?user=postgres&password=XXXX-XXXX-XXXX",
    user = "postgres",
    password = "XXXX-XXXX-XXXX",
    driver = "org.postgresql.Driver"
  }
}
````

## Load to Elasticsearch
Based on the example [Load to parquet](#load-to-parquet) example, we add is the /metadata/sink section to both schemas.

For the sake of the example, we added a field to the location schema to highlight how timestamped indexes may be handled.
Indexes will be named after the domain and schema names suffixed by the timestamp if present.

The `orders` index will be named `sales_orders` and the `customers` index will have a name similar to `sales_customers-2020.01.31`

````yaml
load:
    name: "sales"
    directory: "__COMET_TEST_ROOT__/incoming/sales"
    metadata:
      mode: "FILE"
      format: "DSV"
      withHeader: true
      quote: "\""
      escape: "\\"
      write: "APPEND"
      sink:
        type: BQ
    schemas:
      - name: "customers"
        pattern: "customers-.*.psv"
        metadata:
          separator: "|"
          sink:
            type: ES
            timestamp: "{signup|yyyy.MM.dd}"
        attributes:
          - name: "id"
            type: "customerid"
            required: true
          - name: "signup"
            type: "datetime"
            required: false
          - name: "contact"
            type: "email"
            required: false
          - name: "birthdate"
            type: "date"
            required: false
          - name: "name1"
            type: "string"
            required: false
            rename: "firstname"
          - name: "name2"
            type: "string"
            required: false
            rename: "lastname"
      - name: "orders"
        pattern: "orders-.*.csv"
        merge:
          key:
            - "id"
          delete: "customer_id is null"
        metadata:
          separator: ","
          sink:
            type: ES
        attributes:
          - name: "order_id"
            type: "string"
            required: true
            rename: "id"
          - name: "customer_id"
            type: "customerid"
            required: false
          - name: "amount"
            type: "decimal"
            required: false
          - name: "seller_id"
            type: "string"
            required: false
````

### Custom ES Template

By default, Comet will infer from the dataset schema the properties and their types and create the ES template accordingly.
The default template template is shown below. The variable  `__ATTRIBUTES__` is substituted by the Comet with
the ES representation of the attributes.

````json
{
  "index_patterns": ["{{domain}}.{{schema}}", "{{domain}}.{{schema}}-*"],
  "settings": {
    "number_of_shards": "1",
    "number_of_replicas": "0"
  },
  "mappings": {
    "_doc": {
      "_source": {
        "enabled": true
      },
      "properties": {
        {{attributes}}
      }
    }
  }
}
````

You may customize your ES template by creating a similar file with your own custom properties for a specific schema by putting it
in the file with the following name `COMET_ROOT/metadata/mapping/${domainName}/${schemaName}.json`.

You may inject the domain and schema names using the `{{domain}}` and `{{schema}}` substitution variables.
