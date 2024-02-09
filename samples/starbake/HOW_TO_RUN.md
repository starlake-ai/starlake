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

## Run DAGs

1. Install the dagster webserver
   `python3 -m pip install dagster-webserver`
2. Install the starlake dagster libraries for shell
   `python3 -m pip install starlake-dagster[shell]`
3. Generate DAGs
`./starlake dag-generate --clean`
4. Load the DAGs with dagster
`DAGSTER_HOME=${PWD} dagster dev -f metadata/dags/generated/load/starbake.py -f metadata/dags/generated/transform/CustomerLifetimeValue.py -f metadata/dags/generated/transform/HighValueCustomers.py -f metadata/dags/generated/transform/ProductPerformance.py -f metadata/dags/generated/transform/ProductProfitability.py -f metadata/dags/generated/transform/MostProfitableProducts.py -f metadata/dags/generated/transform/TopSellingProducts.py -f metadata/dags/generated/transform/TopSellingProfitableProducts.py`
