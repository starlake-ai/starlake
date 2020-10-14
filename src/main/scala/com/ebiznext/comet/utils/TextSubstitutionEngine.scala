package com.ebiznext.comet.utils

import scala.annotation.tailrec

/** This class provides a basic text substitution engine, suitable for performing basic variable
  * substitution in a way that is more reliable and faster than applying a sequence of `String#replace`
  * operations.
  *
  * The default start and end variable separators are \_\_ and \_\_, respectively, but can be configured to be
  * anything suitable.
  */
final class TextSubstitutionEngine private (
  substitutions: Map[String, String],
  patternStart: String,
  patternEnd: String
) {

  /** perform variable substitutions on a piece of text content
    * @param sourceContent
    * @return sourceContent after performing substitutions
    */
  def apply(sourceContent: String): String = {
    @tailrec
    def recursiveApply(buffer: StringBuilder, start: Int, nextEvent: Int): String = {
      sourceContent.indexOf(patternStart, start) match {
        case -1 =>
          /* this is it, we're done! */
          buffer.append(sourceContent.substring(start))
          buffer.toString()

        case index =>
          val endIndex = sourceContent.indexOf(patternEnd, index + patternStart.length)
          endIndex match {
            case -1 =>
              throw new IllegalArgumentException(
                s"at position ${index}, unclosed ${patternStart}substitution${patternEnd}\n   in fileContent=${sourceContent}"
              )

            case nextIndex =>
              val substitutionName = sourceContent.substring(index + patternStart.length, nextIndex)
              substitutions.get(substitutionName) match {
                case None =>
                  throw new IllegalArgumentException(
                    s"at position ${index}, unknown substitution ${patternStart}${substitutionName}${patternEnd}\n   in fileContent=${sourceContent}"
                  )

                case Some(substitutionValue) =>
                  buffer.append(sourceContent.substring(start, index))
                  buffer.append(substitutionValue)
                  val nextEvent = sourceContent.indexOf("__", index + patternEnd.length)
                  recursiveApply(buffer, nextIndex + patternEnd.length, nextEvent)
              }
          }
      }
    }

    /* we don't use fileContent.replace(pattern1, value1).replace(pattern2, value2) etc. as:
       1. String#replace internally compiles a regex (!)
       2. it is difficult to manage priorities between patterns and what happens if one pattern match overlaps another
       (e.g if we have ____COMET_TEST_ROOT____ in the resource file, the correct output is __COMET_TEST_ROOT__ not
       __/tmp/foobar/__)
     */

    val firstEvent = sourceContent.indexOf(patternStart)
    if (firstEvent < 0) {
      sourceContent /* NO substitution — break here immediately */
    } else {
      val result = recursiveApply(new StringBuilder, 0, firstEvent)
      result
    }
  }
}

object TextSubstitutionEngine {
  val DefaultSubstitutionPatternStart = "__"
  val DefaultSubstitutionPatternEnd = "__"

  def apply(
    patternStart: String,
    patternEnd: String,
    variables: Seq[(String, String)]
  ): TextSubstitutionEngine =
    new TextSubstitutionEngine(variables.toMap + ("" -> ""), patternStart, patternEnd)

  /** This is the default constructor, where one supplies variable substitutions
    * @param variables any number of variable substitutions to be performed.
    * @return a [[TextSubstitutionEngine]] configured with default substitution pattern start and ends.
    *
    * @note recursive variable substitution is not performed.
    */
  def apply(variables: (String, String)*): TextSubstitutionEngine =
    apply(DefaultSubstitutionPatternStart, DefaultSubstitutionPatternEnd, variables)
}
