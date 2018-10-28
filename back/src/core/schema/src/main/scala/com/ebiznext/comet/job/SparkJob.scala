package com.ebiznext.comet.job

import com.ebiznext.comet.config.{KerberosSession, SparkEnv}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession

trait SparkJob extends StrictLogging {
  def name:String

  lazy val sparkEnv = new SparkEnv(name)
  lazy val session: SparkSession = sparkEnv.session

  def run(args: Array[String]): Unit

  def main(args: Array[String]): Unit = {
    try {
      val principal: Option[String] = Option(
        sparkEnv.config.get("spark.yarn.principal", null))
      val keytab: Option[String] = Option(
        sparkEnv.config.get("spark.yarn.keytab", null))
      logger.info(
        s"principal=$principal/keytab=$keytab=UserGroupInformation.isSecurityEnabled=${UserGroupInformation.isSecurityEnabled}"
      )

      (principal, keytab, UserGroupInformation.isSecurityEnabled) match {
        case (Some(principal), Some(keytab), true) =>
          logger.info(s"principal=$principal / keytab=$keytab")
          KerberosSession.launch(principal, keytab, principal, run, args)
        case (_, _, false) =>
          run(args)
        case (_, _, true) => // Get Keytab from elsewhere
          throw new Exception("missing spark.yarn.principal / spark.yarn.keytab")
      }
    } finally {
      session.stop()
    }
  }
}
