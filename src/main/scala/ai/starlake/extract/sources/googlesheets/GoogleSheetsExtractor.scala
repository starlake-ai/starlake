package ai.starlake.extract.sources.googlesheets

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials

import scala.jdk.CollectionConverters._

/** Configuration for GoogleSheetsExtractor
  *
  * @param id:
  *   id or url
  * @param sheetNames:
  *   sheet names or all sheets by default
  * @param outputDir:
  *   output directory or DatasetArea.extract by default
  * @param outputName:
  *   output name or sheet name by default
  */

case class SpreadsheetConfig(id: String, sheetNames: Option[List[String]] = None)
case class GoogleSheetsExtractorConfig(
  spreadsheets: List[SpreadsheetConfig],
  outputDir: Option[String] = None,
  outputName: Option[String] = None
)

object GoogleSheetsExtractor {
  private val SCOPE = SheetsScopes.SPREADSHEETS_READONLY

  def main(args: Array[String]) = {
    run(
      GoogleSheetsExtractorConfig(
        List(SpreadsheetConfig("1Q179BnuiL_j_YN9owOYxXEmjayU-oi86VWgtn9YUoio"))
      )
    )
  }

  def run(config: GoogleSheetsExtractorConfig) = {
    val APPLICATION_NAME = "GoogleSheets Extractor"
    val HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport
    val credentials = GoogleCredentials.getApplicationDefault

    val spreadsheetId = config.spreadsheets.head.id
    val service =
      new Sheets.Builder(
        HTTP_TRANSPORT,
        JacksonFactory.getDefaultInstance(),
        new HttpCredentialsAdapter(credentials)
      )
        .setApplicationName(APPLICATION_NAME)
        .build();
    val request = service.spreadsheets.get(spreadsheetId)
    val spreadsheet = request.execute
    val spreadsheetUrl = spreadsheet.getSpreadsheetUrl
    val editIndex = spreadsheetUrl.indexOf("/edit")
    val sheets = spreadsheet.getSheets.asScala

    val spreadsheetName = spreadsheet.getProperties.getTitle
    for (sheet <- sheets) {
      val exportUrl = spreadsheetUrl.substring(0, editIndex) +
        s"/export?format=csv&id=$spreadsheetId&gid=${sheet.getProperties.getSheetId}"
      println(s"Exporting sheet ${sheet.getProperties.getTitle} to $exportUrl")
      val sheetName = sheet.getProperties.getTitle
      System.out.println("Sheet Name: " + sheetName)
      val datas = sheet.getData.asScala
      // bien que ça soit une liste, je n'ai toujours eu qu'une seule griddata par sheet
      // peut être mon échantillon de test n'était-il pas assez large
      for (data <- datas) {
        val rowdata = Option(data.getRowData).map(_.asScala).getOrElse(List())
        // me demandez pas pourquoi, il m'est arrivé d'avoir un null, sans savoir d'où ça venait
        /*if (rowdata != null) {
          for (row <- rowdata) {
            for (cell <- row.getValues) {}
          }
        }*/
      }
    }
  }
}
