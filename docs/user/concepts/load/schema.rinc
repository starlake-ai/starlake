.. _schema_concept:

Schema
##########################################


.. option:: name: String

*Required*. Schema name, must be unique among all the schemas belonging to the same domain.
Will become a hive table name On Premise or BigQuery table name on GCP.

.. option:: pattern: String

*Required*. Filename pattern to which this schema must be applied. This may be any Java Regex
This instructs the framework to use this schema to parse any file with a filename that match this pattern.

.. option:: attributes: List[Attribute]

*Required*. Attributes parsing rules.
See :ref:`attribute_concept` for more details.

.. option:: metadata: Metadata

*Optional*. Dataset metadata
See :ref:`metadata_concept` for more details.

.. option:: comment: String

*Optional*. Free text

.. option:: presql: String

*Optional*. Reserved for future use.

.. option:: postsql: String

*Optional*. Reserved for future use.

.. option:: tags: Set[String]

*Optional*. Set of string to attach to this Schema

.. option:: rls: List[RowLevelSecurity]

*Optional*. Experimental. Row level security to apply to this schema once it is ingested.
            This usually execute a set on grants by applying a predicate filter to restrict
            access to a subset of the rows in the table.
            See :ref:`rls_concept` for more details

