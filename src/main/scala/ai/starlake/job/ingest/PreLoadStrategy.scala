package ai.starlake.job.ingest

sealed trait PreLoadStrategy {
  def value: String
}

object PreLoadStrategy {
  case object Imported extends PreLoadStrategy {
    val value: String = "imported"
  }
  case object Ack extends PreLoadStrategy {
    val value: String = "ack"
  }
  case object Pending extends PreLoadStrategy {
    val value: String = "pending"
  }

  def fromString(value: String): Option[PreLoadStrategy] = value.toLowerCase match {
    case Imported.value => Some(Imported)
    case Ack.value      => Some(Ack)
    case Pending.value  => Some(Pending)
    case _              => None
  }
}
