package com.ebiznext.comet.services

import java.io.File

import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.server.{Directives, Route}
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.generator.{FileInput, XlsReader}
import com.ebiznext.comet.schema.generator.YamlSerializer._

class GeneratorService(implicit
  settings: Settings
) extends Directives {

  val tempFile: (FileInfo ⇒ File) = fileInfo => {
    File.createTempFile(fileInfo.getFieldName, fileInfo.getFileName)
  }

  def route: Route = {
    path("generate") {
      post {
        storeUploadedFile(fieldName = "schema", destFn = tempFile) { case (fileInfo, tmpFile) =>
          val reader = new XlsReader(FileInput(tmpFile))
          val result = reader.getDomain().map { domain =>
            serialize(domain)
          }
          complete {
            HttpResponse(
              StatusCodes.OK,
              headers = List(
                RawHeader("fileName", fileInfo.fileName.toLowerCase)
              ),
              entity = HttpEntity(result.getOrElse("")).withoutSizeLimit()
            )
          }
        }
      }
    }
  }

}
