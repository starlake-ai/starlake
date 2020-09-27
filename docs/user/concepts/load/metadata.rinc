.. _metadata_concept:


Metadata
##########################################

Specify Schema properties.
These properties may be specified at the schema or domain level
Any property not specified at the schema level is taken from the
one specified at the domain level or else the default value is returned.

.. option:: mode: Enum

*Optional*. FILE mode by default. FILE and STREAM are the two accepted values. FILE is currently the only supported mode.

.. option:: format: Enum

*Optional*. DSV by default. Supported file formats are :
    - DSV : Delimiter-separated values file. Delimiter value iss specified in the "separator" field.
    - POSITION : FIXED format file where values are located at an exact position in each line.
    - SIMPLE_JSON : For optimisation purpose, we differentiate JSON with top level values from JSON with deep level fields. SIMPLE_JSON are JSON files with top level fields only.
    - JSON :  Deep JSON file. Use only when your json documents contain subdocuments, otherwise prefer to use SIMPLE_JSON since it is much faster.
    - XML : For XML files

.. option:: encoding:

*Optional*. UTF-8 if not specified.

.. option:: multiline: Boolean

*Optional*. Are json objects on a single line or multiple line ? Single by default.  false means single. false also means faster

.. option:: array: Boolean

*Optional*. Is the json stored as a single object array ? false by default. This means that by default we have on json document per line.

.. option:: withHeader: Boolean

*Optional*. When the input file is in the DSV file format, does the dataset has a header ? true bu default

.. option:: separator: String

*Optional*. When the input file is in the DSV file format, yhiss field contains the values delimiter,  ';' by default value may be a multichar string starting from Spark 3

.. option:: quote: Char

*Optional*. The String quote char, '"' by default

.. option:: escape: Char

*Optional*. Escaping char '\' by default

.. option:: write: Enum

*Optional*. Write mode, APPEND by default

.. option:: partition: List[String]

*Optional*. Partition columns, no partitioning by default

.. option:: sink: Sink

*Optional*. Should the dataset be indexed to this sink ?
            See :ref:`sink_concept` for more details.

.. option:: ignore: String

*Optional*. Pattern to ignore or UDF to apply to ignore some lines


