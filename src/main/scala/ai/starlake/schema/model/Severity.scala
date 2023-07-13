package ai.starlake.schema.model

trait Severity
case object Error extends Severity
case object Warning extends Severity
case object Info extends Severity
case object Disabled extends Severity
