println("----------------------------------------------------------------------------")
println("----------------------------AUDIT TABLE-------------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("./quickstart/audit/audit").printSchema
spark.read.parquet("./quickstart/audit/audit").show(false)
