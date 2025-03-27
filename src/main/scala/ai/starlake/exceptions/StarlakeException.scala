package ai.starlake.exceptions

class DisallowRejectRecordException() extends RuntimeException("Fail on rejected count requested")

class DataExtractionException(val domain: String, val table: String)
    extends RuntimeException(s"Data extraction failed for $domain $table")

class SchemaValidationException(val message: String) extends RuntimeException(message)
