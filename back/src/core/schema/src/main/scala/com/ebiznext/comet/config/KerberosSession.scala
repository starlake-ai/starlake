package com.ebiznext.comet.config

import java.security.PrivilegedExceptionAction

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.security.UserGroupInformation

import scala.language.postfixOps

class KerberosSession extends StrictLogging {
  def auth(principal: String, keytab: String): Unit = {
    logger.info(
      s"principal=$principal/keytab=$keytab/isSecurityEnabled=${UserGroupInformation.isSecurityEnabled}"
    )
    if (UserGroupInformation.isSecurityEnabled) {
      UserGroupInformation.loginUserFromKeytab(principal, keytab)
    }
  }

  def launch(principal: String,
             keytab: String,
             proxyUser: String,
             main: Array[String] => _,
             args: Array[String]): Unit = {
    auth(principal, keytab)
    val proxy = UserGroupInformation.createProxyUser(
      proxyUser,
      UserGroupInformation.getCurrentUser())
    proxy.doAs(new PrivilegedExceptionAction[Unit]() {

      override def run(): Unit = {
        logger.info(s"Launching with identity $proxyUser")
        main(args)
      }
    })
  }
}

object KerberosSession extends KerberosSession
