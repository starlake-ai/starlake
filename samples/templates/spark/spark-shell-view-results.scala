println("----------------------------------------------------------------------------")
println("----------------------------SELLERS TABLE-----------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("./quickstart/datasets/accepted/hr/sellers").printSchema
spark.read.parquet("./quickstart/datasets/accepted/hr/sellers").show(false)

println()
println()
println("----------------------------------------------------------------------------")
println("----------------------------LOCATIONS TABLE---------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("./quickstart/datasets/accepted/hr/locations").printSchema
spark.read.parquet("./quickstart/datasets/accepted/hr/locations").show(false)

println()
println()
println("----------------------------------------------------------------------------")
println("----------------------------ORDERS TABLE------------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("./quickstart/datasets/accepted/sales/orders").printSchema
spark.read.parquet("./quickstart/datasets/accepted/sales/orders").show(false)

println()
println()
println("----------------------------------------------------------------------------")
println("----------------------------CUSTOMERS TABLE---------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("./quickstart/datasets/accepted/sales/customers").printSchema
spark.read.parquet("./quickstart/datasets/accepted/sales/customers").show(false)

println()
println()
println("----------------------------------------------------------------------------")
println("----------------------------SALES TABLE-------------------------------------")
println("----------------------------------------------------------------------------")
spark.read.parquet("./quickstart/datasets/business/sales_kpi/byseller_kpi").printSchema
spark.read.parquet("./quickstart/datasets/business/sales_kpi/byseller_kpi").show(false)

