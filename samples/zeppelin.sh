docker run -p 9090:8080 -v $PWD/notebooks:/opt/zeppelin/notebook -v $PWD:/opt/zeppelin/files --rm --name zeppelin apache/zeppelin:0.10.0
# curl 127.0.0.1:9090
