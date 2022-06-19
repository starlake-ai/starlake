println("----------------------------------------------------------------------------")
println("----------------------------AUDIT TABLE-------------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("./quickstart/audit/ingestion-log").printSchema
spark.read.parquet("./quickstart/audit/ingestion-log").show(false)
