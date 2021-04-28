_COMET_VERSION="${COMET_VERSION:-0.1.36}"
_SPARK_VERSION="${SPARK_VERSION:-3.1.1}"
_HADOOP_VERSION="${HADOOP_VERSION:-3.2}"

COMET_JAR_NAME=comet-spark3_2.12-$_COMET_VERSION-assembly.jar
COMET_JAR_FULL_NAME=../bin/$COMET_JAR_NAME
COMET_JAR_URL=https://repo1.maven.org/maven2/com/ebiznext/comet-spark3_2.12/$_COMET_VERSION/$COMET_JAR_NAME

SPARK_TGZ_NAME=spark-$_SPARK_VERSION-bin-hadoop$_HADOOP_VERSION.tgz
SPARK_TGZ_URL=https://downloads.apache.org/spark/spark-$_SPARK_VERSION/$SPARK_TGZ_NAME
SPARK_SUBMIT=../bin/spark-$_SPARK_VERSION-bin-hadoop$_HADOOP_VERSION/bin/spark-submit
SPARK_DIR="$PWD/../bin/spark-$_SPARK_VERSION-bin-hadoop$_HADOOP_VERSION"

initEnv() {
  PROG_DIR=$(cd `dirname $0` && pwd)

  if [ "$PWD" != "$PROG_DIR" ]; then
    echo "run command from local folder in the form ./0.init.sh"
    exit 1
  fi

  rm -rf quickstart/
  mkdir quickstart/

  if [[ ! -d "../bin/" ]]
  then
      mkdir ../bin/
  fi

  if test -f "$COMET_JAR_FULL_NAME"; then
      echo "$COMET_JAR_NAME found in ../bin/"
  else
    echo "downloading $COMET_JAR_NAME from $COMET_JAR_URL"
    curl --output ../bin/
  fi

  if test -f "../bin/$SPARK_TGZ_NAME"; then
      echo "$SPARK_TGZ_NAME found in ../bin/"
  else
    echo "downloading $SPARK_TGZ_NAME from $SPARK_TGZ_URL"
    curl --output ../bin/$SPARK_TGZ_NAME $SPARK_TGZ_URL
    tar zxvf ../bin/$SPARK_TGZ_NAME -C ../bin/
  fi


  cp -r ../quickstart-template/* quickstart/

  awk -v var="$COMET_ROOT" '{sub("__COMET_TEST_ROOT__", var)}1' ../quickstart-template/metadata/domains/hr.comet.yml >quickstart/metadata/domains/hr.comet.yml
  awk -v var="$COMET_ROOT" '{sub("__COMET_TEST_ROOT__", var)}1' ../quickstart-template/metadata/domains/sales.comet.yml >quickstart/metadata/domains/sales.comet.yml
  awk -v var="$COMET_ROOT" '{sub("__COMET_TEST_ROOT__", var)}1' ../quickstart-template/metadata/jobs/kpi.comet.yml >quickstart/metadata/jobs/kpi.comet.yml

  if [[ ! -d "notebooks/" ]]
  then
    mkdir notebooks/
  fi
  if test -f $SPARK_SUBMIT; then
      echo "$SPARK_SUBMIT found in ../bin/"
      echo "Local env initialized correctly"
  else
    echo "$SPARK_SUBMIT not found !!!"
    echo "Failed to initialize environment"
    exit 2

  fi


}
