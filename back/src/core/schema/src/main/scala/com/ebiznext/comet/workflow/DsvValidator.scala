package com.ebiznext.comet.workflow

import java.util.regex.Pattern

import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.Domain
import org.apache.hadoop.fs.Path

class DsvValidator(domain: Domain, schema: SchemaModel.Schema, types: Map[String, Pattern], path: Path) {
}
