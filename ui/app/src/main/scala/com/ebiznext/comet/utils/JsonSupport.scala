package com.ebiznext.comet.utils
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.ext.{JavaTypesSerializers, JodaTimeSerializers}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Formats, _}

import scala.util.Try

trait JsonSupport extends Json4sSupport {

  implicit val serialization = jackson.Serialization

  implicit val formats
    : Formats = DefaultFormats ++ JodaTimeSerializers.all ++ JavaTypesSerializers.all ++ CustomSerializers.all

  def toJSON[T <: AnyRef](o: T): String = serialization.write(o)

  def fromJSON[T <: AnyRef](
    json: String
  )(implicit m: Manifest[T]): Option[T] = {
    val value: JValue = parse(json)
    Try(value.extract[T](formats, m)).toOption

  }
}
