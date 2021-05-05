PROG_DIR=$(cd `dirname $0` && pwd)

if [ "$PWD" != "$PROG_DIR" ]; then
  echo "run command from local folder in the form ./stop.sh"
  exit 1
fi


docker compose down
