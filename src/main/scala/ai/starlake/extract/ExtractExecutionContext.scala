package ai.starlake.extract

import scala.concurrent.ExecutionContext

/** defines a value class that is used by the different method to control parallelism level of
  * extraction in global
  */
class ExtractExecutionContext(val executionContext: Option[ExecutionContext]) extends AnyVal
