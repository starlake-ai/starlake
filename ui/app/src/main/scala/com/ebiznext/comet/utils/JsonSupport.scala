package com.ebiznext.comet.utils
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.ebiznext.comet.model.CometModel.{ Node, Tag }
import spray.json.DefaultJsonProtocol

trait JsonSupport extends SprayJsonSupport {
  import DefaultJsonProtocol._

  implicit val taJsonFormat = jsonFormat2(Tag)
  implicit val nodeJsonFormat = jsonFormat1(Node)
}