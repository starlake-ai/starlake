println("----------------------------------------------------------------------------")
println("----------------------------SELLERS TABLE-----------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("/samples/local/quickstart/datasets/accepted/hr/sellers").printSchema
spark.read.parquet("/samples/local/quickstart/datasets/accepted/hr/sellers").show(false)

println()
println()
println("----------------------------------------------------------------------------")
println("----------------------------LOCATIONS TABLE---------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("/samples/local/quickstart/datasets/accepted/hr/locations").printSchema
spark.read.parquet("/samples/local/quickstart/datasets/accepted/hr/locations").show(false)

println()
println()
println("----------------------------------------------------------------------------")
println("----------------------------CUSTOMERS TABLE---------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("/samples/local/quickstart/datasets/accepted/sales/customers").printSchema
spark.read.parquet("/samples/local/quickstart/datasets/accepted/sales/customers").show(false)

println()
println()
println("----------------------------------------------------------------------------")
println("----------------------------SALES TABLE-------------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("/samples/local/quickstart/datasets/accepted/sales/sales").printSchema
spark.read.parquet("/samples/local/quickstart/datasets/accepted/sales/sales").show(false)

