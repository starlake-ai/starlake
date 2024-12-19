package ai.starlake.schema.exceptions

class InvalidFieldNameException(val fieldName: String)
    extends RuntimeException(
      s"Invalid field name ->${fieldName}<-. Only letters, digits and '_' are allowed. Other characters including spaces are forbidden."
    ) {}
