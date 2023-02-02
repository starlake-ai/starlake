package ai.starlake.schema.model

trait Named {
  def name: String
}

object Named {
  def diff(set1: Set[Named], set2: Set[Named]): Set[Named] =
    set1.filter(e => !set2.map(_.name).contains(e.name))

}

case class NamedValue(name: String, value: AnyRef) extends Named
