---
sidebar_position: 10
---

# Environment
## Env specific variables

Comet allows you to use variables almost everywhere in the domain and job files.
For example, you may need to set the folder name to watch to a different value
in development and production environments. This is where variables may help. They are enclosed inside 
`${}` or `{{}}`

Assuming we have a `sales` domain as follows:
```yaml
name: "sales"
directory: "${root_path}/sales"
ack: "ack"
extensions:
  - "json"
  - "psv"
  - "csv"
  - "dsv"
```

We create a file `env.DEV.comet.yml` in the metadata folder 

```yaml
env:
  root_path: "/tmp/quickstart"
```

and the file `env.PROD.comet.yml` in the metadata folder

```yaml
env:
  root_path: "/cluster/quickstart"
```

To apply the substitution in the DEV env set the COMET_ENV variable before running Comet as follows:

```shell
export COMET_ENV=DEV
```

In Production set it rather to:

```shell
export COMET_ENV=PROD
```

## Global Variables
To define variables accross environment, simply declare them in thet `env.comet.yml` file in the `metadata` folder.

Global variables definition may be superseded by the env specific variables files.  

