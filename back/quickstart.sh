#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo $DIR

rm -r /tmp/metadata /tmp/incoming
rm -r /tmp/datasets
cp -rv $DIR/src/test/resources/sample/quickstart/* /tmp
ls -R /tmp/metadata /tmp/incoming


#java -jar comet-assembly-0.1.jar com.ebiznext.comet.job.Main import

#java -jar comet-assembly-0.1.jar com.ebiznext.comet.job.Main watch