#!/bin/sh
# Usage:
#   ./scripts/redis-cli.sh            # interactive redis-cli
#   ./scripts/redis-cli.sh PING       # run single command non-interactively
#   echo PING | ./scripts/redis-cli.sh  # stdin pipe

if [ -t 0 ]; then
  # stdin is a TTY -> interactive
  exec docker exec -it js-redis redis-cli "$@"
else
  # non-TTY (piped) -> no -t
  exec docker exec -i js-redis redis-cli "$@"
fi
