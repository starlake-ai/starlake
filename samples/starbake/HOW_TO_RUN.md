# starbake

## Prerequisites

Before installing starbake, ensure the following minimum versions are installed on your system:

- jdk: 11 or higher
- python: 3.8 or higher

## Install

1. Install Starlake
  `../../distrib/setup.sh --target=.`
2. Create a virtual environment (optional)
   `python3 -m pip install virtualenv`
   `python3 -m venv .venv`
3. Activate the virtual environment (optional)
   `source .venv/bin/activate`
4. Generate dummy files
   `python3 -m pip install -r _scripts/requirements.txt`
   `python3 _scripts/dummy_data_generator.py`

We're good to go

## Run Starlake

1. Import from incoming to pending
`./starlake import`

2. Load data to bigquery
`./starlake load`

3. Run the transformation in order

```bash
./starlake transform --name Customers.CustomerLifetimeValue 
./starlake transform --name Customers.HighValueCustomers 

./starlake transform --name Products.ProductProfitability 
./starlake transform --name Products.MostProfitableProducts 

./starlake transform --name Products.ProductPerformance 

./starlake transform --name Products.TopSellingProducts 
./starlake transform --name Products.TopSellingProfitableProducts 
```

Notes:

- We should check if connection exists, not just when running load. Maybe sth like starlake compile
- If there is an error of keys in load, the file are already moved to ingesting
- Improve error, for example, if I have connectionRef:  starbake-bigquery, but i forget to create the connection, the error is cannot find key starbake-bigquery. It should be instead, connection starbake-bigquery is not present in connections ...
- audit table should be partitioned
- In audit log, we have -1 in SINK_ACCEPTED & SINK_REJECTED step. Maybe we should just change it to null ?
- Location is EU by default, it should be required (TO TEST)
- transform accept only one file as arg, not many. Can we maybe run all the customers transformations, and it's up to starlake to run them in the right order ?
- If a query ends with ;, transform failed
- Can we disable scala log ? Log only the "business/starlake log" ?
