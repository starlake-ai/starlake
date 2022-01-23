---
sidebar_position: 2
---

# Load

## Load Rules

Load rules are stored in the folder referenced by the COMET_METADATA
environment variable (/tmp/metadata by default).

:::note

You need to export the COMET_METADATA variable before executing any comet load step.

:::

``export COMET_METADATA=hdfs:///my/metadata``

Dataset validation is based on a set of rules we define in schema files.
Schema files describe how the input files are parsed using a set of rules :

* Type Rules: Rules that describe the recognized fields formats.
* Domain Rules: Rules that describe the file format and load strategy
* Schema Rules: Rules that describe field format using pattern matching
* Assertions:  Rules that must be respected by the whole input file. These rules are executed once the file has been ingested.


## Type Rules

Types are defined in the file $COMET_METADATA/types/types.comet.yml.

A type is defined by:

* its name: a string such as "username", "int", "boolean", "long"
* the primitive type it is mapped to. Below is the list of all primitive types:

   * ``string``
   * ``byte``: single char field
   * ``decimal``: For exact arithmetic. Used for money computation
   * ``long``: integers
   * ``double``: floating numbers
   * ``boolean``: boolean values
   * ``date`` : date only fields
   * ``timestamp``: date time fields
* the pattern it should match : A java pattern matching expression that matches the field
   * for types of primitive type "date" or date time, "epoch_milli", "epoch_second" or any predefined or custom date pattern as defined in the [Date Time Formatter](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html#BASIC_ISO_DATE) Specification.

For each primitive type, a type is defined by default. These default types are
located in the file $COMET_METADATA/types/default.comet.yml and they may be redefined
in the file $COMET_METADATA/types/types.comet.yml

File ``$COMET_METADATA/types/default.comet.yml``

```yaml
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
    pattern: "yyyy/MM/dd"
    sample: "2018/07/21"
    comment: "Data in the format yyyy/MM/dd"
- name: "double"
    primitiveType: "double"
    pattern: "-?\\d*\\.{0,1}\\d+"
    sample: "-45.78"
    comment: "Any floating value"
- name: "long"
    primitiveType: "long"
    pattern: "-?\\d+"
    sample: "-64564"
    comment: "any positive or negative number"
- name: "boolean"
    primitiveType: "boolean"
    pattern: "(?i)true|yes|[y1]<-TF->(?i)false|no|[n0]"
    sample: "TruE"
- name: "timestamp"
    primitiveType: "timestamp"
    pattern: "epoch_milli"
    sample: "1548165436433"
    comment: "date/time in epoch millis"
```

Any semantic type that maps to the boolean primitive type must match against a special regex.
This regex is made of two parts separated by the string "<-TF->". values matching the left side will
be interpreted as the boolean value "true" and values matching the right side will be interpreted as the boolean value "false".

We may add new types that map to these primitive types.
For our example above, we will add the following
semantic types to allow better validation on the input fields

$COMET_METADATA/types/types.comet.yml

```yaml
types:
- name: "email"
    primitiveType: "string"
    pattern: "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,6}"
    sample: "me@company.com"
    comment: "Valid email only"
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
```


Now that we have defined the set of semantic
types we want to recognize, we may start defining our schemas.



## Domain Rules


Files are organized by domain. In our example, the "customers" and "orders"
files belong to the "sales" domain  and the "sellers" file belong to the "HR"
domain.

Domain rules are YAML files located in the folder
$COMET_METADATA/domains. They defined :

* The directory where the files coming from this domain are stored
* The ack extension for ack files. "ack" by default.
* Raw file extensions to recognize.  "json", "csv", "dsv", "psv" by default.

The load pipeline also automatically recognize compressed files with
the extension "tgz", "gz" and "zip". These files are uncompressed in a
temporary location and each raw file in the archive is ingested
if the filename matches a file pattern in one of the schema in the domain,
otherwise the file is moved to the "unsolved" folder under the domain name
in the cluster.


The file below explains it all:

File ``$COMET_METADATA/domains/sales.yml``

```yaml
name: "sales"
directory: "/mnt/incoming/sales"
ack: "ack"
extensions:
  - "json"
  - "psv"
  - "csv"
  - "dsv"
```

Using the default values, the definition above may be shortened to :

```yaml
name: "sales"
directory: "/mnt/incoming/sales"
```
This instruct the Starlake Data Pipeline to scan the "/mnt/incoming/sales"
directory and for each file  dataset.ack check for the following files and
ingest it if present :

* dataset.tgz
* dataset.zip
* dataset.gz
* dataset.json
* dataset.csv
* dataset.dsv
* dataset.psv

:::note 

To process files without relying on ack files, simply define the ack attribute with an empty string :```ack : ""```

:::

To ingest files present in the domain incoming directory (/mnt/incoming/sales),
we need to add schema definitions to the domain description file,
aka $COMET_METADATA/domains/sales.yml.


You can define only one domain per YAML domain definition file.

## Schema Rules

A schema is associated to an incoming file if the filename matches the pattern
defined in the schema.
The schema hold the parsing rules through metadata describing the file format
and type mapping rules for each attribute.

First, we add the schema definition to the "customer" file in the domain definition file

File ``$COMET_METADATA/domains/sales.yml``

```yaml

    name: "sales"
    directory: "/mnt/incoming/sales"
    ack: "ack"
    extensions:
      - "json"
      - "psv"
      - "csv"
      - "dsv"
    schemas:
      - name: "customers"
        pattern: "customers-.*.psv"
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
        metadata:
          mode: "FILE"
          format: "DSV"
          withHeader: true
          separator: "|"
          quote: "\""
          escape: "\\"
          write: "APPEND"
```

The schema section in the YAML above should be read as follows :

|Schema|Definition|
|---|---|
pattern|Filename pattern to match in the domain directory
name|Schema name: folder where the dataset is stored and Hive table prefix.
metadata.mode| always FILE. STREAM is reserved for future use.
metadata.format| DSV for delimiter separated values file. SIMPLE_JSON and JSON are also supported.
metadata.withHeader| Does the input file has a header
metadata.separator|What is the field separator
metadata.quote|How are string delimited
metadata.escape|How are characters escaped
metadata.write|Should we APPEND or OVERWRITE existing data in the  cluster
metadata.multiline|"Are JSON object on multiple line. Used when format is JSON or SIMPLE_JSON. This slow down parsing"
metadata.array|"Should we treat the file as a single array of JSON objects. Used  when format is JSON or SIMPLE_JSON and the input data is in brackets [...]"


:::note

Simple JSON are JSON with top level attributes of basic types only. JSON may be used wherever 
you use SIMPLE_JSON but SIMPLE_JSON will make parsing much faster.

:::

Metadata properties may also be defined at the domain level. They will be inherited by all schemas of the domain.
Any metadata property may be redefined at the attribute level.

Each field in the input file is defined using by its name, type and privacy level.
When a header is present, fields do not need to be ordered, since Starlake uses the field name.

The attributes section in the YAML above should be read as follows :


Attribute|definition|
|---|---|
name|Field name as specified in the header. If no header is present, this willthe field name in the ingested dataset.
type|Type as defined in the Type Rules section above.
required|Can this field be empty ?
privacy|How should this field be altered during parsing. May be used to transform the output value.
rename|When header is present in DSV files, this is the new field name in the ingested dataset
metricType|When statistics generation is requested, should this field be treated as continuous, discrete or text value ? Valid values are CONTINUOUS, DISCRETE, TEXT, NONE
array|true when this attribute is an array, false by default
script|Allows you to add a new field computed from a UDF or a Spark SQL built-in standard function


### Privacy Strategy

Default valid values are NONE, HIDE, MD5, SHA1, SHA256, SHA512, AES(not implemented).
Custom values may also be defined by adding a new privacy option in the application.conf. The default reference.conf file defines the following valid privacy
strategies:

```javascript
privacy {
  options = {
    "none": "ai.starlake.privacy.No",
    "hide": "ai.starlake.privacy.Hide",
    "hide10X": "ai.starlake.privacy.Hide(\"X\",10)",
    "approxLong20": "ai.starlake.privacy.ApproxLong(20)",
    "md5": "ai.starlake.privacy.Md5",
    "sha1": "ai.starlake.privacy.Sha1",
    "sha256": "ai.starlake.privacy.Sha256",
    "sha512": "ai.starlake.privacy.Sha512",
    "initials": "ai.starlake.privacy.Initials"
  }
}
```

Any new privacy strategy should implement the following trait :


```scala
/**
    * @param s: String  => Input string to encrypt
    * @param colMap : Map[String, Option[String]] => Map of all the attributes and their corresponding values
    * @param params: List[Any]  => Parameters passed to the algorithm as defined in the conf file.
    *                               Parameter starting with '"' is converted to a string
    *                               Parameter containing a '.' is converted to a double
    *                               Parameter equals to true of false is converted a boolean
    *                               Anything else is converted to an int
    * @return The encrypted string
*/
```

Below, the complete domain definition files.

File ``$COMET_METADATA/domains/sales.yml``

```yaml
    name: "sales"
    directory: "/mnt/incoming/sales"
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
```

:::note

The merge attribute above should be read as follows:

:::

 ```yaml
merge:
  key:
    - "id"
  delete: "customer_id is null"
```

 * When a new orders dataset is imported, only the last occurrence of the record identified by the key column "id" should be kept
 * and any record imported with a null column_id should be removed from the existing dataset.


File ``$COMET_METADATA/domains/hr.yml``

```yaml
name: "hr"
directory: "/mnt/incoming/hr"
metadata:
  mode: "FILE"
  format: "JSON"
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
          - name: "stores"
            type: "string"
            array: true
            required: false
          - name: "country"
            type: "string"
            required: true
```

## Write Strategy

### Partitioning

You may control partitioning strategy and tell Starlake how you wish to partition your
data on disk by specifying one or more attributes in the input file as partition columns.

If you want to use ingestion date/time as partition columns, you can use predefined attributes
``year``, ``month`` ``day``, ``hour``, ``minute`` prefixed by ``comet_``. These columns will
appear as regular attributes in the resulting dataset and without the prefix ``comet_``

Below an example of how to partition by ingestion year, month and day.

```yaml
- metadata:
partition:
    attributes:
      - "comet_year"
      - "comet_month"
      - "comet_day"
```

### Compaction
When saving files as parquet or orc or whatever, the optimal number of partitions depend on the dataset size,
number of records, the size of each record and the block size.

The goal is to optimise the number of partitions during the write phase.


You have 3 choices available :

#### Solution 1 : Naive Compaction
1. Save the file in a temporary location
2. Get the dataset size.
3. Divide the dataset size by the  block size to get the number of partitions
4. Save the dataset to the target location with the computed number of partitions

The main drawback of this approach is that we need to save the file twice.

#### Solution 2 : Sampling
1. Get a percentage of the records in the dataframe before saving it.
2. Save it to a temporary location
3. Estimate the size of the final dataset on HDFS based on the size of the sample
4. Compute the number of partitions based on this estimation
5. Save the dataset to the target location with the computed number of partitions

The Naive solution is in fact identical to the Sampling one with a sampling percentage of 100%.

#### Solution 3 : Absolute Compaction
The number of partitions is defined by the user at the schema level.



Example :

* 0.0 => Means no optimisation.

* 1.0 => Naïve Compaction

* Any integer between 1 ... Int.max => Absolute number of partitions

Below an example of compaction based on a sampling of 20%

```yaml
- metadata:
partition:
    sampling: 0.2 # compute number of partitions based on the size on disk of a sampling of 20% of the dataset
    attributes:
      - "comet_year"
      - "comet_month"
      - "comet_day"
```

With the types catalog, file schemas and save strategy defined we are ready to ingest

## Load Workflow
The ingestion process follows the steps below :

1. Import Step : Files are imported to the cluster in the pending area.
2. Watch Step : Files in the pending area are submitted for ingestion to the Job Manager (Airflow for example).
3. Ingestion Step: Files are validated and converted to a cluster defined file format (parquet, orc ...) and saved as Hive tables.


Before running the steps below, please configure first the environment variables
as described in the Configuration section.

## Import Step

### How it works
1. On startup, all the domain definitions files are loaded from the folder /tmp/metadata/domains
2. Directories referenced by the ``directory`` attribute in the YAML domain definition files are scanned for incoming files. The incoming folder must be accessible locally or through a mount point.
3. Any compressed file or file with any default extension or with one of the extension defined by the ``extensions`` attribute are moved to the cluster in the domain pending folder, /tmp/datasets/pending/``DOMAIN NAME``/ by default.

### Running it
To run the import step, you have to have the spark & hadoop
client libraries in your classpath. You may get them automatically
by running the import step with the spark-submit command:

```shell
$SPARK_HOME/bin/spark-submit comet-assembly-VERSION.jar import
```

## Watch Step

### How it works
The Watch process will scan the all domain pending folders in the cluster.
Any file that matches the pattern defined by the ``pattern`` attribute in
the schema section of the domain definition file is submitted to the Job Manager.
Files that do not match any pattern are moved to the unresolved
folder, /tmp/datasets/unresolved/``DOMAIN NAME``/ by default.

Once copied to the pending folder, a request for ingestion (see step below) is submitted to the Job Manager.

:::note

By default the ``simple`` job manager is invoked. This simple manager 
used for debugging & testing purpose would launch the ingestion step inside the current process. 
In production, you would configure a job manager running on your cluster. 
Starlake comes with the ``airflow`` job manager and sample DAGs required to run all three steps.

:::

### Running it
To run the import step, you have to have the spark & hadoop
client libraries in your classpath. You may get them automatically
by running the watch step with the spark-submit command:

```shell
$SPARK_HOME/bin/spark-submit comet-assembly-VERSION.jar watch
```

## Ingestion Step

### How it works
Unlike the steps above, this step does not scan any folder.
It takes as its parameters the domain name, schema name and
full path of the file that need to be ingested. That's why it is usually
invoked through request submitted to a job manager by at the Watch Step.


### Running it
To interactively run it, copy the input file in the pending area
of a domain and run it as follows:

```shell
$ SPARK_HOME/bin/spark-submit comet-assembly-VERSION.jar ingest DOMAIN_NAME SCHEMA_NAME hdfs://datasets/domain/pending/file.dsv
```

## Export Step


### How it works
This step is concerned with exporting the dataset to Elasticsearch / SQL Database / CSV or JSON file
It takes as its parameters the domain name, schema name and
full path of the file that need to be ingested. That's why it is usually
invoked through request submitted to a job manager by at the Watch Step.


### Running it
To interactively run it, copy the input file in the pending area
of a domain and run it as follows:

```shell
$ SPARK_HOME/bin/spark-submit comet-assembly-VERSION.jar ingest DOMAIN_NAME SCHEMA_NAME hdfs://datasets/domain/pending/file.dsv
```

