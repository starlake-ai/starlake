package ai.starlake.exceptions

class NullValueFoundException(val nbRecord: Long)
    extends RuntimeException(s"Found $nbRecord null values")
class DisallowRejectRecordException() extends RuntimeException("Fail on rejected count requested")

class DataExtractionException(val domain: String, val table: String)
    extends RuntimeException(s"Data extraction failed for $domain $table")
