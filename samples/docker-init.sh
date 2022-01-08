#!/usr/bin/env bash

echo "Setting up samples ..."
rm -rf /samples/bin
mkdir -p /samples/bin
cp /app/starlake-spark3_2.12-0.2.8-SNAPSHOT-assembly.jar /samples/bin/
cp -r /app/spark-3.1.2-bin-hadoop3.2 /samples/bin/
HOSTNAM=`cat /etc/hostname`
echo "setup finished"
echo "to login run: docker exec -it $HOSTNAM bash"

mainmenu () {
  read -n 1 -p "Press x to exit container:" mainmenuinput
  if [ "$mainmenuinput" = "x" ]; then
    echo
  elif [ "$mainmenuinput" = "X" ];then
    echo
  else
    echo "Invalid input!"
    echo "Please try again!"
    mainmenu
  fi
}

mainmenu
