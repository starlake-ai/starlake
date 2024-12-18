package ai.starlake.core.utils

import java.text.Normalizer

object StringUtils {
  private val OpenAPIParameterRegex = "\\{[^}]*}".r
  private val AccentRegex = "\\p{M}".r
  private val NonAlphanumericRegex = "[^\\p{Alnum}]".r
  private val ConsecutiveUnderscoreRegex = "_+".r
  private val TrailingUnderscoreRegex = "^_*(.*?)_*$".r

  /** Removes OpenAPI-style parameters from a route. Example: "route/{id}" → "route/" */
  def removeOpenAPIParameters(route: String): String = {
    OpenAPIParameterRegex.replaceAllIn(route, "")
  }

  /** Removes accents from a string. Example: "éàô" → "eao" */
  def removeAccents(input: String): String = {
    val normalized = Normalizer.normalize(input, Normalizer.Form.NFD)
    AccentRegex.replaceAllIn(normalized, "")
  }

  /** Replaces non-alphanumeric characters in a string with underscores. Example: "a!b@c" → "a_b_c"
    */
  def replaceNonAlphanumericWithUnderscore(input: String): String = {
    NonAlphanumericRegex.replaceAllIn(input, "_")
  }

  /** Replaces consecutive underscores with a single underscore. Example: "a__b___c" → "a_b_c" */
  def replaceConsecutiveUnderscores(input: String): String = {
    ConsecutiveUnderscoreRegex.replaceAllIn(input, "_")
  }

  /** Removes trailing/leading underscores. Example: "_a_b_c_" → "a_b_c" */
  def removeTrailingUnderscore(input: String): String = {
    TrailingUnderscoreRegex.replaceFirstIn(input, "$1")
  }

  /** Adds underscores to camel case naming. Examples:
    *   - "camelCase" → "camel_Case"
    *   - "XMLParser" → "XML_Parser"
    */
  def addUnderscores(input: String): String = {
    input
      .replaceAll("([a-z])([A-Z])", "$1_$2") // Add underscore before capitals preceded by lowercase
      .replaceAll("([A-Z])([A-Z][a-z])", "$1_$2") // Add underscore after a group of capitals
  }
}
