#!/usr/bin/env bash

HOSTNAM=`cat /etc/hostname`
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
