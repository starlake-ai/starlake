package ai.starlake.utils

import ai.starlake.schema.model.Domain

object CaseClassDoc {
  import scala.reflect.runtime.universe._

  /** Generates documentation and data models of case classes.
    *
    * @param caseClass
    *   The case class to generate documentation and data models for.
    * @tparam T
    *   The type of the case class.
    */
  def generateCaseClassDocumentation[T: TypeTag](caseClass: T): Unit = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val classSymbol = typeOf[T].typeSymbol.asClass
    val classMirror = mirror.reflectClass(classSymbol)

    // Generate documentation
    val classAnnotations = classSymbol.annotations
    val className = classSymbol.name.toString
    val classDocumentation = classAnnotations.map(_.tree.toString).mkString("\n")
    println(s"Documentation for $className:\n$classDocumentation")

    // Generate data model
    val classFields = classSymbol.typeSignature.decls.collect {
      case field: TermSymbol if field.isVal || field.isVar => field
    }
    val fieldNames = classFields.map(_.name.toString)
    val fieldTypes = classFields.map(_.typeSignature.toString)
    println(s"Data model for $className:")
    fieldNames.zip(fieldTypes).foreach { case (fieldName, fieldType) =>
      println(s"$fieldName: $fieldType")
    }
  }

  // Example usage

  /** A case class to generate documentation and data models for.
    * @param name:
    *   The name of the person.
    * @param age:
    *   The age of the person.
    */
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit =
    generateCaseClassDocumentation(Domain(""))

}
