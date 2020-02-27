#!/usr/bin/env bash
#https://community.cloudera.com/t5/Community-Articles/Creating-Custom-Types-and-Entities-in-Atlas/ta-p/245046
#https://atlas.apache.org/api/v2/resource_TypesREST.html#resource_TypesREST_createAtlasTypeDefs_POST
#https://community.cloudera.com/t5/Support-Questions/How-to-get-bulk-entities-using-v2-Rest-Endpoints-of-Atlas-0/td-p/189770
#http://atlas.apache.org/api/v2/

curl -X POST -u admin:admin -H "Cache-Control: no-cache" http://127.0.0.1:21000/api/atlas/admin/import -d /Users/hayssams/programs/apache-atlas-2.0.0/models/4000-Comet/4010-common_typedefs.json
curl -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin:admin -H "Cache-Control: no-cache" http://127.0.0.1:21000/api/atlas/v2/types/typedefs


curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin:admin 'http://localhost:21000/api/atlas/v2/types/typedefs' -d @/Users/hayssams/programs/apache-atlas-2.0.0/models/4000-Comet/4010-common_typedefs.json


curl -X POST -u admin:admin -H "Cache-Control: no-cache" http://34.98.120.196/api/atlas/admin/import -d /Users/hayssams/programs/apache-atlas-2.0.0/models/4000-Comet/4010-common_typedefs.json
curl -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin:admin -H "Cache-Control: no-cache" http://34.98.120.196/api/atlas/v2/types/typedefs


curl -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' -u admin:admin 'http://34.98.120.196/api/atlas/v2/types/typedefs' -d @/Users/hayssams/programs/apache-atlas-2.0.0/models/4000-Comet/4010-common_typedefs.json