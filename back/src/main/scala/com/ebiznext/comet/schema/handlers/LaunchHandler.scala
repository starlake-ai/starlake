package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.config.Settings.comet
import com.ebiznext.comet.job.Main
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Schema}
import okhttp3._
import okio.Buffer
import org.apache.hadoop.fs.Path

import scala.util.{Success, Try}

trait LaunchHandler {
  def ingest(domain: Domain, schema: Schema, path: Path): Boolean = ingest(domain, schema, path :: Nil)

  def ingest(domain: Domain, schema: Schema, path: List[Path]): Boolean
}



class SimpleLauncher extends LaunchHandler {
  override def ingest(domain: Domain, schema: Schema, paths: List[Path]): Boolean = {
    paths.foreach { path =>
      val params =  Array("ingest", domain.name, schema.name, path.toString)
      Main.main(params)
    }
    true
  }
}

class AirflowLauncher extends LaunchHandler {
  def post(url: String, json: String): Try[String] = {
    Try {
      val JSON: MediaType = MediaType.parse("application/json; charset=utf-8")
      val client: OkHttpClient = new OkHttpClient
      val body: RequestBody = RequestBody.create(JSON, json)
      val request: Request = new Request.Builder().url(url).post(body).build
      val buffer = new Buffer()
      request.body().writeTo(buffer)
      println(request.toString + "\n" + buffer.readUtf8())
      val response: Response = client.newCall(request).execute
      response.body.string
    }
  }

  override def ingest(domain: Domain, schema: Schema, path: List[Path]): Boolean = {
    val endpoint = Settings.comet.airflow.endpoint
    val url = s"$endpoint/dags/comet_ingest/dag_runs"
    val command = s"""ingest ${domain.name} ${schema.name} ${path.mkString(",")}"""
    val json =s"""{"conf":"{\\"command\\":\\"$command\\"}"}"""
    post(url, json) match {
      case Success(response) =>
        println(response)
      case scala.util.Failure(exception) => throw exception
    }
    true
  }
}




