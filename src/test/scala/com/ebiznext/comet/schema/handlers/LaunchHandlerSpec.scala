package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.TestHelper
import org.scalatest.{FlatSpec, Matchers}

class LaunchHandlerSpec extends TestHelper {
  "Launch" should "Airflow task" in {
    val launch = new AirflowLauncher
  }
}
