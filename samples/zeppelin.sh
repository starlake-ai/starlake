docker run -p 9090:8080 -v ${PWD}:/samples -v ${PWD}/notebooks:/zeppelin/notebook -v ${PWD}:/zeppelin/files --rm --name zeppelin apache/zeppelin:0.10.0
# curl 127.0.0.1:8080
