.. _attribute_concept:

*********************************************
Attribute
*********************************************

A field in the schema. For struct fields, the field "attributes" contains all sub attributes

.. option:: name: String

Attribute name as defined in the source dataset and as received in the file

.. option:: type: String

Semantic type of the attribute.

.. option:: array: Boolean

Is it an array ?

.. option:: required: Boolean

Should this attribute always be present in the source

.. option:: privacy:PrivacyLevel

Should this attribute be applied a privacy transformation at ingestion time

.. option:: comment: String

free text for attribute description

.. option:: rename: String

If present, the attribute is renamed with this name

.. option:: metricType:MetricType

If present, what kind of stat should be computed for this field

.. option:: attributes: List[Attribute]

List of sub-attributes (valid for JSON and XML files only)

.. option:: position: Position

Valid only when file format is POSITION

.. option:: default: String

Default value for this attribute when it is not present.

.. option:: tags:Set[String]

Tags associated with this attribute

.. option:: trim: Trim

Should we trim the attribute value ?

.. option:: script: String

Scripted field : SQL request on renamed column
