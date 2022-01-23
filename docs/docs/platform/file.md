---
sidebar_position: 5
title: Local filesystem
---


## Sample setup
If running the samples on MacOS or Linux, you may skip this section.

To run the samples locally on Windows, you must first create the docker image :

````shell
$ cd samples
$ docker build --build-arg STARLAKE_VERSION=0.2.8 -t starlake .
````

One the docker image is built locally, run it :

````shell
$ docker run -it starlake:latest
Setting up samples ...
setup finished
to login run: docker exec -it 51c331b7e757 bash
Press x to exit container:
````
From another window terminal, enter the docker container (replace 51c331b7e757 by the container id returned above) :

````shell
$ docker exec -it 51c331b7e757 bash
````

## Running the samples

Make sure you are in the `samples/local` folder
The quickstart-template is first duplicated into the samples/local folder to create a startup project  

````shell
$ ./0.data-init.sh
````
Then you need to import he files located in quickstart/incoming into the correct pending folder depending on the domain they belong to:
````shell
$ ./1.data-import.sh
````
To start the ingestion process, run the load command. The resulting tables should be available in the quickstart/datasets/accepted folder :
````shell
$ ./2.data-load.sh
````

To join multiple datasets using the KPI job example located in quickstart/metadata/jobs/kpi.sql, run the corresponding transformation :
````shell
$ ./3.data-transform.sh
````


To view the data ingested and stored as parquet files :
````shell
$ ./4.data-view-results.sh
````
To exit the spark shell above type `:quit`

To view the log produced :
````shell
$ ./4.data-view-audit.sh
````
To exit the spark shell above type `:quit`


## Optional

You may view the relationship between your tables by generating a graphviz diagram using the command below:
````shell
$ ./1.data-visualization.sh
````





