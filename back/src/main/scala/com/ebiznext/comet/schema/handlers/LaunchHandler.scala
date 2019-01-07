package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.job.Main
import com.ebiznext.comet.schema.model.{Domain, Schema}
import com.typesafe.scalalogging.StrictLogging
import okhttp3._
import okio.Buffer
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}

trait LaunchHandler {
  def ingest(domain: Domain, schema: Schema, path: Path): Boolean = ingest(domain, schema, path :: Nil)
  def ingest(domain: Domain, schema: Schema, path: List[Path]): Boolean
}


class SimpleLauncher extends LaunchHandler with StrictLogging {
  override def ingest(domain: Domain, schema: Schema, paths: List[Path]): Boolean = {
    paths.foreach { path =>
      val params = Array("ingest", domain.name, schema.name, path.toString)
      logger.info(s"Launch Ingestion: ${params.mkString(" ")}")
      Main.main(params)
    }
    true
  }
}

class AirflowLauncher extends LaunchHandler with StrictLogging {
  def post(url: String, json: String): Try[String] = {
    Try {
      val JSON: MediaType = MediaType.parse("application/json; charset=utf-8")
      val client: OkHttpClient = new OkHttpClient
      val body: RequestBody = RequestBody.create(JSON, json)
      val request: Request = new Request.Builder().url(url).post(body).build
      val buffer = new Buffer()
      request.body().writeTo(buffer)
      logger.debug("Post to Airflow: " + request.toString + "\n" + buffer.readUtf8())
      val response: Response = client.newCall(request).execute
      response.body.string
    }
  }

  override def ingest(domain: Domain, schema: Schema, path: List[Path]): Boolean = {
    val endpoint = Settings.comet.airflow.endpoint
    val url = s"$endpoint/dags/comet_ingest/dag_runs"
    val command = s"""ingest ${domain.name} ${schema.name} ${path.mkString(",")}"""
    val json =s"""{"conf":"{\\"command\\":\\"$command\\"}"}"""
    logger.info(s"Post to Airflow: $json")
    post(url, json) match {
      case Success(response) =>
        logger.info(s"Airflow returned: $response")
      case Failure(exception) => throw exception
    }
    true
  }
}




