package com.ebiznext.comet.services

import java.io.File

import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.server.{Directives, Route}
import com.ebiznext.comet.config.Settings
import com.ebiznext.comet.schema.generator.Xls2Yml.{genPostEncryptionDomain, genPreEncryptionDomain}
import com.ebiznext.comet.schema.generator.{FileInput, XlsReader}
import com.ebiznext.comet.schema.generator.YamlSerializer._

class GeneratorService(implicit
  settings: Settings
) extends Directives {

  val tempFile: (FileInfo ⇒ File) = fileInfo => {
    File.createTempFile(fileInfo.getFieldName, fileInfo.getFileName)
  }

  def route: Route = {
    path("generate-schema") {
      post {
        storeUploadedFile(fieldName = "schema", destFn = tempFile) { case (fileInfo, tmpFile) =>
          val reader = new XlsReader(FileInput(tmpFile))
          val result = reader.getDomain().map { domain =>
            serialize(domain)
          }
          result match {
            case Some(success) =>
              complete {
                HttpResponse(
                  StatusCodes.OK,
                  headers = List(
                    RawHeader("fileName", fileInfo.fileName.toLowerCase)
                  ),
                  entity = HttpEntity(success).withoutSizeLimit()
                )
              }
            case _ =>
              complete {
                HttpResponse(
                  StatusCodes.BadRequest,
                  entity =
                    HttpEntity("Please check your Yaml configuration file").withoutSizeLimit()
                )
              }
          }

        }
      }
    } ~
    path("encrypt-schema") {
      post {
        formFields('privacy.optional, 'delimiter.optional) { (privacy, delimiter) =>
          storeUploadedFile(fieldName = "schema", destFn = tempFile) { case (fileInfo, tmpFile) =>
            val result = new XlsReader(FileInput(tmpFile)).getDomain().map { domain =>
              val privacies = privacy match {
                case Some(prv) => prv.split(",").toList
                case _         => Nil
              }
              val preEncrypt = serialize(genPreEncryptionDomain(domain, privacies))
              val postEncrypt = serialize(genPostEncryptionDomain(domain, delimiter, privacies))
              s"""Pre-encrypt Schema :
                  | $preEncrypt
                  | 
                  | Post-encrypt Schema :
                  | $postEncrypt
                  |""".stripMargin
            }
            result match {
              case Some(success) =>
                complete {
                  HttpResponse(
                    StatusCodes.OK,
                    headers = List(
                      RawHeader("fileName", fileInfo.fileName.toLowerCase)
                    ),
                    entity = HttpEntity(success).withoutSizeLimit()
                  )
                }
              case _ =>
                complete {
                  HttpResponse(
                    StatusCodes.BadRequest,
                    entity =
                      HttpEntity("Please check your Yaml configuration file").withoutSizeLimit()
                  )
                }
            }
          }
        }
      }
    }
  }

}
