package ai.starlake.core.utils

import scala.jdk.CollectionConverters._

object CaseClassToPojoConverter {
  // used to transform case classes to java map and have easy usage in jinjava

  private def convertCollectionElements(collection: IterableOnce[Any]): Any = {
    collection match {
      case o: Option[_] if o.exists(_.isInstanceOf[Product]) =>
        o.map(el => CaseClassToPojoConverter.asJava(el.asInstanceOf[Product])).orNull[Any]
      case o: Option[_] =>
        o.map {
          case i: IterableOnce[_] =>
            convertCollectionElements(i)
          case e => e
        }.orNull[Any]
      case s: Seq[_] if s.exists(_.isInstanceOf[Product]) =>
        s.map(el => CaseClassToPojoConverter.asJava(el.asInstanceOf[Product])).asJava
      case s: Seq[_] =>
        s.map {
          case i: IterableOnce[_] =>
            convertCollectionElements(i)
          case e => e
        }.asJava
      case s: Set[_] if s.exists(_.isInstanceOf[Product]) =>
        // convert set to list in order to have the ability in templating to use index based
        s.toList.map(o => CaseClassToPojoConverter.asJava(o.asInstanceOf[Product])).asJava
      case s: Set[_] =>
        s.toList.map {
          case i: IterableOnce[_] =>
            convertCollectionElements(i)
          case e => e
        }.asJava
      case s: Map[_, _] =>
        s.map { case (k, v) =>
          val javaKey = k match {
            case p: Product         => CaseClassToPojoConverter.asJava(p)
            case i: IterableOnce[_] => convertCollectionElements(i)
            case _                  => k
          }
          val javaValue = v match {
            case p: Product         => CaseClassToPojoConverter.asJava(p)
            case i: IterableOnce[_] => convertCollectionElements(i)
            case _                  => v
          }
          javaKey -> javaValue
        }.asJava
    }
  }

  def asJava[T <: Product](obj: T): java.util.Map[String, Any] = {
    obj.productElementNames
      .zip(obj.productIterator.map {
        case o: Option[_] if o.exists(_.isInstanceOf[Product]) =>
          o.map(el => CaseClassToPojoConverter.asJava(el.asInstanceOf[Product])).orNull[Any]
        case o: Option[_] =>
          o.map {
            case i: Iterable[_] =>
              convertCollectionElements(i)
            case e => e
          }.orNull[Any]
        case i: Iterable[_] =>
          convertCollectionElements(i)
        case c: Product => CaseClassToPojoConverter.asJava(c) // Nested case class conversion
        case other      => other
      })
      .toMap
      .asJava
  }
}
