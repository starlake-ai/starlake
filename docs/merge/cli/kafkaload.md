## Samples

### Batch offload topic from kafka to a file


Assume we want to periodically offload an avro topic to the disk and create a filename with the date time, the batch was started.
We need to provide the following input parameters to the starlake batch:

- offload: We set it to true since we are consuming data from kafka
- mode: Overwrite since we are creating a unique file for each starlake batch
- path: the file path where the consumed data will be stored. We can use here any standard starlake variable, for example `/tmp/file-{{comet_datetime}}.txt`
- format: We may save it in any spark supported format (parquet, text, json ...)
- coalesce: Write all consumed messages into a single file if set to 1
- config: The config entry on the application.conf describing the topic connections options. See below.


