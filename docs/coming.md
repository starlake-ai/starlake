# Coming articles

- A starlake is born: How starlake came to life
- How to test your datawarehouse SQL scripts at zero cost
- How starlake makes it possible to move your code from Snowflake / Amazon Redshift or Databricks to BigQuery that easy 
- How starlake achieve unparallelled performance on data extraction
- How starlake achieve fast data loading while still doing thorough data validation
- How bad is it to load your data and validate it later
- How Starlake moves your data from one database to another incrementally and incredibly fast
- How Starlake makes it possible to run SQL transforms on multiple environment without the refs
- Why (dbt) refs are not compatible with serious software engineering and how starlake solves this
- SQLGlot transpiler (python shop) vs JSQLTranspiler (Java shop): how good or bad are they ?
- Why did we choose scala over Python to write Starlake
- Why did we choose Scala Tapir as our web framework instead of a Python alternative ?
- Data transformation: How Starlake keeps your code simple whatever complex your materialization strategy is ?

## A starlake is born: How starlake came to life


### The story
In 2015, I was running ebiznext a company specialized in data engineering.
My historical customer needed to build a big data platform to store and analyze their data.
The technology they were willing to use was Hadoop and Spark and when they started the tender, they were looking for a company that could help them build this platform.
We were a small company (20 people) and our competitors were big companies (with more than a 100.000 headcount).

I needed to find a way to differentiate us from the competition and I decided to build a data platform that would be built many times faster than the competition.

We needed to be abel to load data from many sources and all the competitors came with a solution that was based on ETL tools where tha tiem required to build
a data pipeline was proportional to the number of sources that needed to be loaded.

That's where I decided to gather a team of the best people in town and we started to think of a way to build a data platform that would be able to load data from any source in a matter of minutes
without any ETL tool or data engineering skills.


The day of the tender, 
we presented our solution and the customer was amazed by the speed at which we were able to load data from any source as we promised to be in production
in a matter of weeks while the competition was promising to be in production in a matter of months.

We won the tender (a multi year contract of more than 5000 days) and we started to build the platform that would be the foundation of Starlake.

While all the competition was presenting a solution based on Cloudera or Hortonworks, we decided to build our platform on top of
Apache Mesos, Apache Spark (Core & Streaming), Apache Hive, Apache Kafka and Elasticsearch. Everything was running in Docker containers and we were able to deploy the platform in a matter of minutes using Ansible scripts.

### The birth of Smartlake

The basic idea behind Smartlake was to be able to describe the input format using a simple JSON file and
then the platform would be able to load the data and store it as spark / Hive tables.






