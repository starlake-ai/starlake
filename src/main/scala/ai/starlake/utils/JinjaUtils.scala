package ai.starlake.utils

import ai.starlake.config.Settings
import ai.starlake.utils.Formatter.RichFormatter
import com.hubspot.jinjava.{Jinjava, JinjavaConfig}

import scala.jdk.CollectionConverters._

object JinjaUtils {

  private var _jinjava: Jinjava = null

  def jinjava(implicit settings: Settings): Jinjava = {
    if (_jinjava == null) {
      val curClassLoader = Thread.currentThread.getContextClassLoader
      val res =
        try {
          Thread.currentThread.setContextClassLoader(this.getClass.getClassLoader)
          val config = JinjavaConfig
            .newBuilder()
            .withFailOnUnknownTokens(false)
            .withNestedInterpretationEnabled(false)
            .build()

          new Jinjava(config)
        } finally Thread.currentThread.setContextClassLoader(curClassLoader)
      res.setResourceLocator(new JinjaResourceHandler())
      _jinjava = res
    }
    _jinjava
  }

  def resetJinjaClassLoader(): Unit = {
    _jinjava = null
  }

  def renderJinja(str: String, params: Map[String, Any] = Map.empty)(implicit
    settings: Settings
  ): String = {
    jinjava.render(str, params.asJava)
  }

  def parseJinja(str: String, params: Map[String, Any])(implicit
    settings: Settings
  ): String =
    parseJinja(
      List(str),
      params
    ).head

  def parseJinja(str: List[String], params: Map[String, Any])(implicit
    settings: Settings
  ): List[String] = {
    val result = str.map { sql =>
      renderJinja(sql, params)
        .richFormat(params, Map.empty)
        .trim
    }
    result
  }

  def parseJinjaTpl(
    templateContent: String,
    params: Map[String, Object]
  )(implicit
    settings: Settings
  ): String = {
    parseJinja(templateContent, params)
  }
}