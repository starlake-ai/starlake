docker run -p 8080:8080 -v $PWD/notebooks:/zeppelin/notebook -v $PWD:/zeppelin/files --rm --name zeppelin apache/zeppelin:0.9.0
# curl 127.0.0.1:8080
