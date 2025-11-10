import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object BigqueryLoad {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession
      .builder()
      .appName("BigQueryExample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    conf.set("fs.gs.project.id", "starlake-325712") // actual project filled in
    conf.set("google.cloud.auth.service.account.enable", "true")
    conf.set("google.cloud.auth.type", "APPLICATION_DEFAULT")
    conf.set(
      "google.cloud.auth.service.account.json.keyfile",
      "/Users/hayssams/.gcloud/keys/starlake-hayssams.json"
    ) // actual keyfile filled in
    println("Spark Version -> " + spark.version)
    // Define the schema
    val schema = StructType(
      Array(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("test", StringType, nullable = false)
      )
    )

    // Create a DataFrame with the specified schema
    val data = Seq(
      ("Alice2", 30, None),
      ("Bob2", 25, Some("new"))
    )

    val df = spark.createDataFrame(data).toDF("name", "age", "test")

    df.printSchema()

    // Write DataFrame to BigQuery
    df.write
      .format("bigquery")
      .option("project", "starlake-325712")
      .option("dataset", "audit")
      .option("location", "europe-west1")
      .option("writeMethod", "indirect")
      .option("temporaryGcsBucket", "starlake-app")
      .option("table", "audit.bqload")
      .option("writeDisposition", "WRITE_APPEND") // Append mode
      .option("createDisposition", "CREATE_IF_NEEDED") // Create if needed
      .option("authType", "APPLICATION_DEFAULT")
      .option("authScopes", "https://www.googleapis.com/auth/cloud-platform")
      // .option("autodetect", "true")
      // .option("writeMethod", "indirect")
      .option("allowFieldAddition", "true") // Allow adding new fields
      .option("allowFieldRelaxation", "true") // Allow relaxing fields
      .mode("append")
      .save()

    // Stop the Spark session
    spark.stop()
  }
}
