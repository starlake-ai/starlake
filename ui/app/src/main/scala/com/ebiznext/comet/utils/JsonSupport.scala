package com.ebiznext.comet.utils
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.json4s.ext.{JavaTypesSerializers, JodaTimeSerializers}
import org.json4s.{DefaultFormats, Formats, jackson}

trait JsonSupport extends Json4sSupport {
  implicit val serialization = jackson.Serialization

  implicit val formats
    : Formats = DefaultFormats ++ JodaTimeSerializers.all ++ JavaTypesSerializers.all ++ CustomSerializers.all
}
