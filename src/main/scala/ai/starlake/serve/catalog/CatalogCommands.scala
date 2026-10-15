package ai.starlake.serve.catalog

import ai.starlake.config.Settings

import scala.util.Try

object CatalogCommands {

  def isCatalogCommand(name: String): Boolean = name.startsWith("catalog-")

  def handle(args: Array[String])(implicit settings: Settings): String =
    handle(args, new CatalogService(settings.appConfig.connections))

  def handle(args: Array[String], service: CatalogService): String = {
    val options = parseOptions(args.toList.drop(1))
    def required(name: String): String =
      options.getOrElse(
        name,
        throw new CatalogException(CatalogErrorCode.BadRequest, s"Missing argument --$name")
      )
    def limit: Int =
      options
        .get("limit")
        .map { value =>
          value.toIntOption.getOrElse(
            throw new CatalogException(CatalogErrorCode.BadRequest, s"Invalid --limit '$value'")
          )
        }
        .getOrElse(CatalogService.DefaultPreviewLimit)

    CatalogJson.fromTry(Try {
      args.headOption.getOrElse("") match {
        case "catalog-connections" => service.listConnections()
        case "catalog-schemas"     => service.listSchemas(required("connection"))
        case "catalog-tables"      => service.listTables(required("connection"), required("schema"))
        case "catalog-columns" =>
          service.listColumns(required("connection"), required("schema"), required("table"))
        case "catalog-preview" =>
          service.preview(required("connection"), required("schema"), required("table"), limit)
        case other =>
          throw new CatalogException(CatalogErrorCode.BadRequest, s"Unknown command '$other'")
      }
    })
  }

  /** `--key value` pairs. A key without a value is ignored. */
  private[catalog] def parseOptions(args: List[String]): Map[String, String] =
    args
      .sliding(2, 1)
      .collect { case key :: value :: Nil if key.startsWith("--") && !value.startsWith("--") =>
        key.drop(2) -> value
      }
      .toMap
}
