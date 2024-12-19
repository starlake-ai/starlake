package ai.starlake.extract.impl.openapi

import ai.starlake.core.utils.NamingUtils.normalizeTableName
import io.swagger.v3.oas.models.media.{Schema => OpenAPISwaggerSchema}
import io.swagger.v3.oas.models.responses.ApiResponse
import io.swagger.v3.oas.models.{Operation, PathItem}

import java.util.regex.Pattern

/** Represents metadata information about an API path.
  *
  * @constructor
  *   Creates an instance of PathInfo with the specified parameters.
  * @param apiPath
  *   The API endpoint path.
  * @param description
  *   An optional description providing additional information about the API path.
  * @param summary
  *   An optional summary briefly describing the API path.
  * @param pathItem
  *   The PathItem object containing detailed information about the API path operations.
  */
protected case class PathInfo(
  apiPath: String,
  description: Option[String],
  summary: Option[String],
  pathItem: PathItem
)

/** Represents an HTTP operation mapped to a specific API path, encapsulating the related metadata
  * about the operation, such as tags, descriptions, and associated operation details.
  *
  * @constructor
  *   Creates a new instance of `PathOperation` with the specified tags, method, description,
  *   summary, and operation.
  * @param tags
  *   A list of tags that categorize or group this operation.
  * @param method
  *   The HTTP method (GET, POST, etc.) associated with this operation.
  * @param description
  *   An optional detailed description of the operation.
  * @param summary
  *   An optional brief summary of the operation.
  * @param operation
  *   The underlying operation details for this API endpoint.
  */
protected case class PathOperation(
  tags: List[String],
  method: HttpOperation,
  description: Option[String],
  summary: Option[String],
  operation: Operation
)

/** Represents the response structure for a specific API path.
  *
  * @param status
  *   The HTTP status code associated with the response.
  * @param description
  *   An optional detailed description of the response.
  * @param summary
  *   An optional brief summary of the response.
  * @param apiResponse
  *   The API response object containing additional details.
  */
protected case class PathResponse(
  status: Int,
  description: Option[String],
  summary: Option[String],
  apiResponse: ApiResponse
)

/** Represents the schema for an HTTP response in the context of an OpenAPI specification.
  *
  * @param mediaType
  *   The media type of the response, such as `application/json` or `text/plain`.
  * @param schemaDescription
  *   An optional description of the schema to provide additional context or details.
  * @param schema
  *   The underlying OpenAPISchema that defines the structure and constraints of the response.
  * @param schemaNames
  *   A list of schema names associated with the response.
  */
protected case class ResponseSchema(
  mediaType: String,
  schemaDescription: Option[String],
  schema: OpenAPISwaggerSchema[_],
  schemaNames: List[String]
)

/** Represents an enriched OpenAPI schema which includes schema names and provides functionality for
  * pattern matching with those schema names.
  *
  * @param schema
  *   The OpenAPI schema object.
  * @param schemaNames
  *   List of names associated with the schema.
  */
protected case class RichOpenAPISchema(schema: OpenAPISwaggerSchema[_], schemaNames: List[String]) {
  lazy val schemaNamesStr: String = if (schemaNames.isEmpty) {
    "UndefinedSchemaName"
  } else {
    schemaNames.mkString(", ")
  }

  def notMatchWith(schemaPatterns: List[Pattern]): Boolean = !matchWith(schemaPatterns)

  def matchWith(schemaPatterns: List[Pattern]): Boolean = {
    schemaNames.foldLeft(false) { case (acc, schemaName) =>
      acc || schemaPatterns.exists(_.matcher(schemaName).matches())
    }
  }
}

/** Represents essential information extracted from an API definition. Provides methods to generate
  * a description and a normalized table name based on the API's path, attribute path, and optional
  * rename rules. It also offers utilities to match API paths against specified patterns.
  *
  * @constructor
  *   Creates an instance of ApiEssentialInformation with the provided details about an API.
  * @param tags
  *   A list of tags associated with the API operation.
  * @param schema
  *   The schema describing the API's request or response structure.
  * @param apiMethod
  *   The HTTP method used for the API operation, represented as `HttpOperation`.
  * @param apiPath
  *   The path of the API operation.
  * @param attributePath
  *   An optional attribute path that is relevant during operations such as schema explosion.
  * @param attributeDescription
  *   An optional description of the attribute, used when schema explosion is applied.
  * @param schemaDescription
  *   An optional description of the schema.
  * @param responseDescription
  *   An optional description of the API response.
  * @param operationDescription
  *   An optional description of the API operation.
  * @param pathDescription
  *   An optional description of the path itself.
  */
protected case class ApiEssentialInformation(
  tags: List[String],
  schema: RichOpenAPISchema,
  apiMethod: HttpOperation,
  apiPath: String,
  attributePath: Option[String],
  attributeDescription: Option[String], // this is used only when explode is done
  schemaDescription: Option[String],
  responseDescription: Option[String],
  operationDescription: Option[String],
  pathDescription: Option[String]
) {
  def description: Option[String] = {
    attributeDescription
      .orElse(schemaDescription)
      .orElse(responseDescription)
      .orElse(operationDescription)
      .orElse(pathDescription)
  }

  def buildTableName(
    apiBasePathOpt: Option[String],
    rename: Option[String],
    attributePathRename: Map[String, Pattern]
  ): String = {
    val nameToNormalize: String = rename
      .getOrElse {
        val attributeRenamed = attributePath
          .flatMap { ap =>
            attributePathRename
              .find { case (_, p) => p.matcher(ap).matches() }
              .map(_._1)
          }
        attributeRenamed
          .filter(_.nonEmpty)
          .getOrElse(
            apiBasePathOpt
              .map(apiBasePath => apiPath.replaceFirst("^" + apiBasePath, ""))
              .getOrElse(apiPath) + attributeRenamed.map("_" + _).getOrElse("")
          )
      }
    normalizeTableName(nameToNormalize)
  }

  def notMatchWith(pathPatterns: List[Pattern]): Boolean = !matchWith(pathPatterns)

  def matchWith(pathPatterns: List[Pattern]): Boolean = {
    pathPatterns.exists(_.matcher(apiPath).matches())
  }
}
