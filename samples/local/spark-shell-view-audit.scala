println("----------------------------------------------------------------------------")
println("----------------------------AUDIT TABLE-------------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("/samples/local/quickstart/audit/ingestion-log").printSchema
spark.read.parquet("/samples/local/quickstart/audit/ingestion-log").show(false)
