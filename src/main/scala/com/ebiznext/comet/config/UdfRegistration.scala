package com.ebiznext.comet.config

import org.apache.spark.sql.SparkSession

trait UdfRegistration {
  def register(session: SparkSession): Unit
}
