package com.ebiznext.comet.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.PropertySource
@Configuration
@ConfigurationProperties
//@PropertySource("classpath:application.properties")
class Configuration {
  private var env: String = null
  def getEnv(): String = { this.env }

}
