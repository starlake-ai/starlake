package ai.starlake.utils

import scala.collection.mutable
import scala.reflect.runtime.universe.runtimeMirror
import scala.tools.reflect.ToolBox

object CompilerUtils {
  var compiledCode = mutable.Map[String, Any]()
  def compile[A](code: String): (Map[String, Any]) => A = {
    val cachedWrapper =
      if (compiledCode.contains(code)) {
        compiledCode(code)
      } else {
        val tb = runtimeMirror(getClass.getClassLoader).mkToolBox()
        val tree = tb.parse(s"""
           |def wrapper(context: Map[String, Any]): Boolean = {
           |val count = context("count").asInstanceOf[Long]
           |val result = context("result").asInstanceOf[Seq[Any]]
           |val results = context("results").asInstanceOf[Seq[Seq[Any]]]
           |$code
           |}
           |wrapper _
            """.stripMargin)
        val f = tb.compile(tree)
        val wrapper: Any = f()
        compiledCode += (code -> wrapper)
        wrapper
      }
    cachedWrapper.asInstanceOf[Map[String, Any] => A]
  }
}
