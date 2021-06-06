COMET_VERSION="${COMET_VERSION:-0.2.5-SNAPSHOT}"
SPARK_VERSION="${SPARK_VERSION:-3.1.1}"
HADOOP_VERSION="${HADOOP_VERSION:-3.2}"

COMET_JAR_NAME=comet-spark3_2.12-$COMET_VERSION-assembly.jar
COMET_JAR_FULL_NAME=../bin/$COMET_JAR_NAME

echo "COMET_VERSION=$COMET_VERSION"
echo "SPARK_VERSION=$SPARK_VERSION"
echo "HADOOP_VERSION=$HADOOP_VERSION"

if [[ "$COMET_VERSION" == *"SNAPSHOT"* ]]; then
  COMET_JAR_URL=https://oss.sonatype.org/content/repositories/snapshots/com/ebiznext/comet-spark3_2.12/$COMET_VERSION/$COMET_JAR_NAME
else
  COMET_JAR_URL=https://repo1.maven.org/maven2/com/ebiznext/comet-spark3_2.12/$COMET_VERSION/$COMET_JAR_NAME
fi

echo "COMET_JAR_URL=$COMET_JAR_URL"
SPARK_DIR_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARK_TGZ_NAME=$SPARK_DIR_NAME.tgz
SPARK_TGZ_URL=https://downloads.apache.org/spark/spark-$SPARK_VERSION/$SPARK_TGZ_NAME
SPARK_SUBMIT=../bin/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION/bin/spark-submit
SPARK_DIR="$PWD/../bin/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION"

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
      echo $COMET_JAR_FULL_NAME
      echo "$COMET_JAR_NAME found in ../bin/"
  else
    echo "downloading $COMET_JAR_NAME from $COMET_JAR_URL"
    curl --output ../bin/$COMET_JAR_NAME $COMET_JAR_URL
  fi

  if test -f "../bin/$SPARK_TGZ_NAME"; then
      echo "$SPARK_TGZ_NAME found in ../bin/"
  else
    echo "downloading $SPARK_TGZ_NAME from $SPARK_TGZ_URL"
    curl --output ../bin/$SPARK_TGZ_NAME $SPARK_TGZ_URL
    tar zxvf ../bin/$SPARK_TGZ_NAME -C ../bin/
  fi

  rm ../bin/$SPARK_DIR_NAME/conf/*.xml

  cp -r ../quickstart-template/* quickstart/

  awk -v var="$COMET_ROOT" '{sub("__COMET_TEST_ROOT__", var)}1' ../quickstart-template/metadata/env.comet.yml >quickstart/metadata/env.comet.yml

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
