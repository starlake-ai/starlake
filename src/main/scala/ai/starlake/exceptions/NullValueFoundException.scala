package ai.starlake.exceptions

class NullValueFoundException(val nbRecord: Long)
    extends RuntimeException(s"Found $nbRecord null values")
class DisallowRejectRecordException() extends RuntimeException("Fail on rejected count requested")
