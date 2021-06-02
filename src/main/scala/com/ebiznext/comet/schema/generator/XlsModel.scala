package com.ebiznext.comet.schema.generator

trait XlsModel {

  val allDomainHeaders = List(
    "_name"        -> "Name",
    "_path"        -> "Directory",
    "_ack"         -> "Ack",
    "_description" -> "Description"
  )

  val allSchemaHeaders = List(
    "_name"               -> "Name",
    "_pattern"            -> "Pattern",
    "_mode"               -> "FILE or STREAM",
    "_write"              -> "Write Mode\n(OVERWRITE, APPEND, ERROR_IF_EXISTS)",
    "_format"             -> "DSV, POSITION, XML, JSON",
    "_header"             -> "Hash header (true / false)",
    "_delimiter"          -> "Separator",
    "_delta_column"       -> "Timestamp column to use on merge",
    "_merge_keys"         -> "Merge columns",
    "_description"        -> "Description",
    "_encoding"           -> "File encoding (UTF-8 by default)",
    "_sampling"           -> "Sampling strategy",
    "_partitioning"       -> "partition columns",
    "_sink"               -> "Sink Type",
    "_clustering"         -> "Clustering columns",
    "_merge_query_filter" -> "Filter to use on merge"
  )

  val allAttributeHeaders = List(
    "_name"           -> "Name",
    "_rename"         -> "New Name",
    "_type"           -> "Semantic Type",
    "_required"       -> "Required(true / false)",
    "_privacy"        -> "Privacy (MD5, SHA1, Initials ...)",
    "_metric"         -> "Metric (CONTINUOUS, DISCRETE ...)",
    "_default"        -> "Default value",
    "_script"         -> "Script",
    "_description"    -> "Description",
    "_position_start" -> "Start Position",
    "_position_end"   -> "End Position",
    "_trim"           -> "Trim (LEFT, RIGHT,BOTH)",
    "_ignore"         -> "Ignore ?"
  )

}
