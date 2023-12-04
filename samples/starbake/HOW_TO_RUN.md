Thi

## Install 

1. Install Starlake
    `SL_VERSION=1.0.0-SNAPSHOT sh /Users/elarib/Work/Starlake/starlake/distrib/starlake.sh`
2. Generate dummy files
   `pip install faker`
   `python _scripts/dummy_data_generator.py`

We're good to go


## Run Starlake 

1. Import from incoming to pending
`sh starbake.sh import`


2. Load data to bigquery
`sh starbake.sh load`

3. Run the transformation in order
```
sh starbake.sh transform --name Customers.CustomerLifetimeValue 
sh starbake.sh transform --name Customers.HighValueCustomers 

sh starbake.sh transform --name Products.ProductProfitability 
sh starbake.sh transform --name Products.MostProfitableProducts 

sh starbake.sh transform --name Products.ProductPerformance 

sh starbake.sh transform --name Products.TopSellingProducts 
sh starbake.sh transform --name Products.TopSellingProfitableProducts 
```






Notes:
. We should check if connections exists, not just when running load. Maybe sth like starlake compile
. If there is was an error of keys in load, the file are already moved to ingesting
. Improve error, for example, if i have connectionRef:  starbake-bigquery, but i forget to create the create connection, the error is cannot find key starbake-bigquery. It should be instead, connection starbake-bigquery is not present in connections ...
. audit table should be partionned
. In audit log, we have -1 in SINK_ACCEPTED & SINK_REJECTED step. Maybe we should just change it to null ?
. Location is EU by default, it should be required (TO TEST)
. transform accept only one file as arg, not many. Can we maybe run all the customers transfromations, and it's up to starlake to run them in the right order ?
. If a query ends with ;, transform failed
. Can we disable scala log ? Log only the "business/starlake log" ?
.
