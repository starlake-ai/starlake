package ai.starlake.serve

import scala.jdk.CollectionConverters._

/** Reads the JSON body of a `POST /api/v1/cli` request.
  *
  * Two argument formats are accepted: `PARAMS`, a space-separated string (legacy clients), and
  * `ARGUMENTS`, a JSON array of strings, either encoded as a string or inline (the VS Code
  * extension). `PARAMS` wins when both are present.
  */
object RequestParams {

  def parsePayload(json: String): Map[String, Any] =
    SingleUserMainServer.mapper
      .readValue(json, classOf[java.util.Map[String, Any]])
      .asScala
      .toMap

  def string(payload: Map[String, Any], key: String): Option[String] =
    payload.get(key).flatMap(Option(_)).map(_.toString)

  def args(payload: Map[String, Any]): Array[String] =
    string(payload, "PARAMS").filter(_.trim.nonEmpty) match {
      case Some(params) => params.split(" ")
      case None =>
        payload.get("ARGUMENTS").flatMap(Option(_)) match {
          case Some(encoded: String) =>
            SingleUserMainServer.mapper.readValue(encoded, classOf[Array[String]])
          case Some(list: java.util.List[_]) => list.asScala.map(String.valueOf).toArray
          case Some(seq: Iterable[_])        => seq.map(String.valueOf).toArray
          case _ =>
            throw new IllegalArgumentException("Request must contain PARAMS or ARGUMENTS")
        }
    }
}
