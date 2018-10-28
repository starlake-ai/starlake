package com.ebiznext.comet.schema.handlers

import org.apache.hadoop.fs.Path

trait LaunchHandler {
  def launch(domain: String, schema: String, path: Path): Boolean
}
