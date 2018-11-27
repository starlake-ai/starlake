package com.ebiznext.comet.schema.handlers

import java.io.InputStream

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.sample.SampleData
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}

class LaunchHandlerSpec extends FlatSpec with Matchers with SampleData {
  "Launch" should "Airflow task" in {
    val launch = new AirflowLauncher
    launch.ingest("", "", new Path("/"))
  }
}
