package ai.starlake.core.utils

import StringUtils.{
  addUnderscores,
  removeAccents,
  removeOpenAPIParameters,
  removeTrailingUnderscore,
  replaceConsecutiveUnderscores,
  replaceNonAlphanumericWithUnderscore
}

/** Utility object for normalizing and formatting string-based identifiers such as table or
  * attribute names.
  */
object NamingUtils {

  /** Normalizes a table name by performing the following operations:
    *   - Removes OpenAPI-style parameters.
    *   - Removes accents from characters.
    *   - Replaces non-alphanumeric characters with underscores.
    *   - Replaces consecutive underscores with a single underscore.
    *   - Removes trailing and leading underscores.
    *   - Converts camel case to snake case by adding underscores.
    *   - Converts the result to lowercase.
    *
    * @param input
    *   The input string representing the table name to normalize.
    * @return
    *   A normalized string suitable for use as a table name.
    */
  def normalizeTableName(input: String): String = {
    addUnderscores(
      removeTrailingUnderscore(
        replaceConsecutiveUnderscores(
          replaceNonAlphanumericWithUnderscore(removeAccents(removeOpenAPIParameters(input)))
        )
      )
    ).toLowerCase()
  }

  /** Normalizes an attribute name by performing the following operations:
    *   - Removes accents from characters.
    *   - Replaces non-alphanumeric characters with underscores.
    *   - Replaces consecutive underscores with a single underscore.
    *   - Removes trailing and leading underscores.
    *   - Adds underscores to camel case naming.
    *
    * @param input
    *   The input string representing the attribute name to normalize.
    * @return
    *   A normalized string suitable for use as an attribute name.
    */
  def normalizeAttributeName(input: String, toSnakeCase: Boolean = true): String = {
    val sanitizedName = removeTrailingUnderscore(
      replaceConsecutiveUnderscores(
        replaceNonAlphanumericWithUnderscore(removeAccents(input))
      )
    )
    if (toSnakeCase) addUnderscores(sanitizedName)
    else sanitizedName
  }
}
