Complete Example
================

Problem description
-------------------

Say we have to ingest customers, orders and sellers into the datalake.
The customers and orders are provided by the "sales" department while
the sellers dataset is provided by the HR departement.


The customers and orders dataset are sent by the "sales" department
as CSV  files. Below is an extract of these files.

``File customers-2018-05-10.psv from "sales" departement``

.. code-block:: text

 id|signup|contact|birthdate|name1|name2
 A009701|2010-01-31 23:04:15|me@home.com|1980-10-14|Donald|Obama
 B308629|2016-12-01 09:56:02|you@land.com|1980-10-14|Barack|Trump

``File orders-2018-05-10.psv from the "sales" department``

.. code-block:: text

 
 order_id,customer_id,amount,seller_id
 12345,A009701,123.65,AQZERD
 56432,B308629,23.8,AQZERD

.. note::
 Before sending the files, the "sales" department zip all its files
 into a single compressed files and put them in the folder /mnt/incoming/sales of the landing area.

The sellers dataset is sent as JSON array by the HR department.

``File sellers-2018-05-10.json from the HR department``

.. code-block:: json

 [
     { "id":"AQZERD", "seller_email":"me@acme.com", "location_id": 1},
     { "id":"TYUEZG", "seller_email":"acme.com","location_id": 2 }

 ]

``File locations-2018-05-10.json from the HR department``

.. code-block:: json

    {
        "id":1,
        "address": {
            "city":"Paris",
            "stores": ["Store 1", "Store 2", "Store 3"]
            "country":"France"
        }
    }
    {
        "id":2,
        "address": {
            "city":"Berlin",
            "country":"Germany"
        }
    }




.. note::
 the HR department does not zip its files. It simply copy them into the
 folder /mnt/incoming/hr of the landing area.

.. warning::
 We intentionnally set an invalid email for the second seller.


Ingestion Rules
---------------

Ingestion rules are stored in the HDFS folder referenced by the COMET_METADATA
environment variable (/tmp/metadata by default).

.. note::
 You need to export this variable before executing any comet ingestion step.
 ``export COMET_METADATA=hdfs:///my/metadata``

Dataset validation is based on a set of rules we define in schema files.
Schema files decribe how the input files are parsed using a set of rules :

* Type Rules: Rules that describe the recognized fields formats.
* Domain Rules: Rules that describe the file format and ingestion strategy
* Schema Rules: Rules that describe field format using pattern matching


Type Rules
~~~~~~~~~~

Types are defined in the HDFS file $COMET_METADATA/types/types.yml.

A type is defined by:

* its name: a string such as "username", "int", "boolean", "long"
* the primitive type it is mapped to. Below is the list of all primitive types:

   * ``string``
   * ``byte``: single char field
   * ``decimal``: For exact arithmetic. Used for money computation
   * ``long```: integers
   * ``double``: floating numbers
   * ``boolean```: boolean values
   * ``date``` : date only fields
   * ``timestamp``: date time fields
* the pattern it should match : A java pattern matching expression that matches the field

For each primitive type, a type is defined by default. These default types are
located in the file $COMET_METADATA/types/default.yml and they may be redefined
in the file $COMET_METADATA/types/types.yml

``File $COMET_METADATA/types/default.yml``

.. code-block:: yaml

    types:
    - name: "string"
        primitiveType: "string"
        pattern: ".+"
        sample: "Hello World"
        comment: "Any set of chars"
    - name: "byte"
        primitiveType: "byte"
        pattern: "."
        sample: "x"
        comment: "Any set of chars"
    - name: "date"
        primitiveType: "date"
        pattern: "(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\\\d\\\\d)"
        sample: "2018/07/21"
        comment: "Data in the format yyyy/MM/dd"
    - name: "double"
        primitiveType: "double"
        pattern: "-?\\d*\\.{0,1}\\d+"
        sample: "-45.78"
        comment: "Any flating value"
    - name: "double"
        primitiveType: "double"
        pattern: "-?\\d*\\.{0,1}\\d+"
        sample: "-45.787686786876"
        comment: "Any flating value"
    - name: "long"
        primitiveType: "long"
        pattern: "-?\\d+"
        sample: "-64564"
        comment: "any positive or negative number"
    - name: "boolean"
        primitiveType: "long"
        pattern: "(?i)true|false|yes|no|[yn01]"
        sample: "TruE"
    - name: "timestamp"
        primitiveType: "timestamp"
        pattern: "\\d+"
        sample: "1548165436433"
        comment: "date/time in epoch millis"


We may add new types that map to these primitive types.
For our example above, we will add the following
semantic types to allow better validation on the input fields

``File $COMET_METADATA/types/types.yml``

.. code-block:: yaml

    types:
    - name: "email"
        primitiveType: "string"
        pattern: "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,6}"
        sample: "me@company.com"
        comment: "Valid email only"
    types:
    - name: "customerid"
        primitiveType: "string"
        pattern: "[A-Z][0-9]{6}"
        sample: "A123456"
        comment: "Letter followed by 6 digits"
    - name: "sellerid"
        primitiveType: "string"
        pattern: "[0-9]{6}"
        sample: "123456"
        comment: "6 digits string"

Now that we have defined the set of semantic
types we want to recognize, we may start defining our schemas.


Domain Rules
~~~~~~~~~~~~

Files are organized by domain. In our example, the "customers" and "orders"
files belong to the "sales" domain  and the "sellers" file belong to the "HR"
domain.

Domain rules are YAML files located in the folder
$COMET_METADATA/domains. They defined :

* The directory where the files coming from this domain are stored
* The ack extension for ack files. "ack" by default
* Raw file extensions to recognize.  "json", "csv", "dsv", "psv" by default.

The ingestion pipeline also automatically recognize compressed files with
the extension "tgz", "gz" and "zip". These files are uncompressed in a
temporary location and each raw file in the archive is ingested
if the filename matches a file pattern in one of the schema in the domain,
otherwise the file is moved to the "unsolved" folder under the domain name
in the cluster.


The file below explains it all:

``File $COMET_METADATA/domains/sales.yml``

.. code-block:: yaml

    name: "sales"
    directory: "/mnt/incoming/sales"
    ack: "ack"
    extensions:
      - "json"
      - "psv"
      - "csv"
      - "dsv"

Using the default values, the definition above may be shortened to :

.. code-block:: yaml

    name: "sales"
    directory: "/mnt/incoming/sales"

This instruct the Comet Data Pipeline to scan the "/mnt/incoming/sales"
directory and for each file  dataset.ack check for the following files and
ingest it if present :

* dataset.tgz
* dataset.zip
* dataset.gz
* dataset.json
* dataset.csv
* dataset.dsv
* dataset.psv

To ingest files present in the domain incoming directory (/mnt/incoming/sales),
we need to add schema definitions to the domain description file,
aka $COMET_METADATA/domains/sales.yml.


You can define only one domain per YAML domain definition file.

Schema Rules
~~~~~~~~~~~~

A schema is associated to an incoming file if the filename matches the pattern
defined in the schema.
The schema hold the parsing rules through metadata describing the file format
and type mapping rules for each attribute.

First, we add the schema definition to the "customer" file in the domain definition file

``File $COMET_METADATA/domains/sales.yml``

.. code-block:: yaml

    name: "sales"
    directory: "/mnt/incoming/sales"
    ack: "ack"
    extensions:
      - "json"
      - "psv"
      - "csv"
      - "dsv"
    schema:
      - name: "customers"
        pattern: "customers-.*.dsv"
        metadata:
          mode: "FILE"
          format: "DSV"
          withHeader: true
          separator: "|"
          quote: "\""
          escape: "\\"
          write: "APPEND"
        attributes:
          - name: "id"
            type: "string"
            required: true
            privacy: "NONE"
          - name: "signup"
            type: "datetime"
            required: false
            privacy: "NONE"
          - name: "contact"
            type: "email"
            required: false
            privacy: "NONE"
          - name: "name1"
            type: "string"
            required: false
            privacy: "NONE"
            rename: "firstname"
          - name: "name2"
            type: "string"
            required: false
            privacy: "NONE"
            rename: "lastname"
          - name: "birthdate"
            type: "date"
            required: false
            privacy: "HIDE"

The schema section in the YAML above should be read as follows :

.. csv-table:: Schema definition
   :widths: 20, 60

   pattern,Filename pattern to match in the domain directory
   name, Schema name: HDFS folder where the dataset is stored and Hive table prefix.
   metadata.mode, always FILE. STREAM is reserved for future use.
   metadata.format, DSV for delimiter separated values file. SIMPLE_JSON and JSON are also supported.
   metadata.withHeader, Does the input file has a header
   metadata.separator, What is the field separator
   metadata.quote, How are string delimited
   metadata.escape, How are characters escaped
   metadata.write, Should we APPEND or OVERWRITE existing data in the HDFS cluster
   metadata.multiline, "Are JSON objects on multiple line. Used when format is JSON or SIMPLE_JSON. This slow down parsing"
   metadata.array, "Should we treat the file as an array of objects. Used  when format is JSON or SIMPLE_JSON"


.. note::
   Simple JSON are JSON with top level attributes of basic types only. JSON may be used wherever
   you use SIMPLE_JSON but SIMPLE_JSON will make parsing much faster.

Metadata properties may also be defined at the domain level. They will be inherited by all schemas of the domain.
Any metadata property may be redefined at the attribute level.

Each field in the input file is defined using by its name, type and privacy level.
When a header is present, fields do not need to be ordered, since Comet uses the field name.

The attributes section in the YAML above should be read as follows :


.. csv-table:: Attribute definition
   :widths: 20, 60

   name, "Field name as specified in the header. If no header is present, this willthe field name in the ingested dataset."
   type, Type as defined in the Type Rules section above.
   required, Can this field be empty ?
   privacy, "How should this field be protected. Valid values are NONE, HIDE, MD5, SHA1, SHA256, SHA512, AES(not impemented)"
   rename, "When header is present, this is the new field name in the ingested dataset"
   stat, "When statistics generation is requested, should this field be treated as continous, discrete or text value ? Valid values are CONTINUOUS, DISCRETE, TEXT, NONE"
   array, "true when this attribute is an array, false by default"


Below, he complete domain definition files.

``File $COMET_METADATA/domains/sales.yml``

.. code-block:: yaml

    name: "sales"
    directory: "/mnt/incoming/sales"
    metadata:
      mode: "FILE"
      format: "DSV"
      withHeader: true
      quote: "\""
      escape: "\\"
      write: "APPEND"
    schema:
      - name: "customers"
        pattern: "customers-.*.dsv"
        metadata:
          separator: "|"
        attributes:
          - name: "id"
            type: "string"
            required: true
          - name: "signup"
            type: "datetime"
            required: false
          - name: "contact"
            type: "email"
            required: false
          - name: "name1"
            type: "string"
            required: false
            rename: "firstname"
          - name: "name2"
            type: "string"
            required: false
            rename: "lastname"
          - name: "birthdate"
            type: "date"
            required: false
            privacy: "HIDE"
      - name: "orders"
        pattern: "orders-.*.dsv"
        metadata:
          separator: "|"
        attributes:
          - name: "order_id"
            type: "string"
            required: true
            rename: "id"
          - name: "customer_id"
            type: "string"
            required: false
          - name: "amount"
            type: "decimal"
            required: false
          - name: "seller_id"
            type: "string"
            required: false



``File $COMET_METADATA/domains/hr.yml``

.. code-block:: yaml

    name: "hr"
    directory: "/mnt/incoming/hr"
    metadata:
      mode: "FILE"
      write: "APPEND"
    schema:
      - name: "sellers"
        pattern: "sellers-.*.dsv"
        metadata:
          array: true
          format: "SIMPLE_JSON"
        attributes:
          - name: "id"
            type: "string"
            required: true
          - name: "seller_email"
            type: "email"
            required: true
          - name: "location_id"
            type: "int"
            required: true
      - name: "locations"
        pattern: "locations-.*.dsv"
        metadata:
          format: "JSON"
          multiline: true
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
              - name: "stores"
                type: "string"
                array: true
                required: false
              - name: "country"
                type: "string"
                required: true



With the types catalog and file schemas defined we are ready to ingest