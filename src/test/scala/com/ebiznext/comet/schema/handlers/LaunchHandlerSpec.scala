package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.sample.SampleData
import org.scalatest.{FlatSpec, Matchers}

class LaunchHandlerSpec extends FlatSpec with Matchers with SampleData {
  "Launch" should "Airflow task" in {
    val launch = new AirflowLauncher
  }
}
