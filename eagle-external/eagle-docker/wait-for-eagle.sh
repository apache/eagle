#!/bin/bash

: ${EAGLE_HOST:=$AMBARISERVER_PORT_9099_TCP_ADDR}
: ${SLEEP:=2}
: ${DEBUG:=1}

: ${EAGLE_HOST:? eagle server address is mandatory, fallback is a linked containers exposed 9099}

debug() {
  [ $DEBUG -gt 0 ] && echo [DEBUG] "$@" 1>&2
}

get-server-state() {
  curl -s -o /dev/null -w "%{http_code}" $AMBARISERVER_PORT_9099_TCP_ADDR:9099/eagle-service/index.html
}

debug waits for eagle to start on: $EAGLE_HOST
while ! get-server-state | grep 200 &>/dev/null ; do
  [ $DEBUG -gt 0 ] && echo -n .
  sleep $SLEEP
done
[ $DEBUG -gt 0 ] && echo
debug eagle web started: $EAGLE_HOST:9099/eagle-service
