package ai.starlake.utils

import scala.collection.mutable
import scala.reflect.runtime.universe.runtimeMirror
import scala.tools.reflect.ToolBox

object CompilerUtils {
  private val compiledCode = mutable.Map[String, Any]()
  def compileExpectation(code: String): (Map[String, Any]) => Boolean = {
    compile(s"""
      |def wrapper(context: Map[String, Any]): Boolean = {
      |val count = context("count").asInstanceOf[Long]
      |val result = context("result").asInstanceOf[Seq[Any]]
      |val results = context("results").asInstanceOf[Seq[Seq[Any]]]
      |$code
      |}
      |wrapper _
      """.stripMargin)
  }

  def compileWriteStrategy(code: String): (Map[String, Any]) => Boolean = {
    compile(s"""
      |def wrapper(context: Map[String, Any]): Boolean = {
      |  def group(groupVal: Any): String = {
      |    val m = context("matcher").asInstanceOf[java.util.regex.Matcher]
      |    groupVal match {
      |      case s: String => m.group(s)
      |      case i: Int => m.group(i)
      |      case _ => throw new RuntimeException("Support only String or Int")
      |    }
      |  }
      |  val isFirstDayOfMonth = context("isFirstDayOfMonth").asInstanceOf[Boolean]
      |  val isLastDayOfMonth = context("isLastDayOfMonth").asInstanceOf[Boolean]
      |  val dayOfWeek = context("dayOfWeek").asInstanceOf[Int]
      |  val isFileFirstDayOfMonth = context("isFileFirstDayOfMonth").asInstanceOf[Boolean]
      |  val isFileLastDayOfMonth = context("isFileLastDayOfMonth").asInstanceOf[Boolean]
      |  val fileDayOfWeek = context("fileDayOfWeek").asInstanceOf[Int]
      |  val fileSize = context("fileSize").asInstanceOf[Long]
      |  val fileSizeB = fileSize
      |  val fileSizeKo = fileSizeB / 1024L
      |  val fileSizeMo = fileSizeKo / 1024L
      |  val fileSizeGo = fileSizeMo / 1024L
      |  val fileSizeTo = fileSizeGo / 1024L
      |  $code
      |}
      |wrapper _
      """.stripMargin)
  }

  private def compile[A](code: String): Map[String, Any] => A = {
    val cachedWrapper =
      if (compiledCode.contains(code)) {
        compiledCode(code)
      } else {
        val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
        val tree = tb.parse(code)
        val f = tb.compile(tree)
        val wrapper: Any = f()
        compiledCode += (code -> wrapper)
        wrapper
      }
    cachedWrapper.asInstanceOf[Map[String, Any] => A]
  }
}
