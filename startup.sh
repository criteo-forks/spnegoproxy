#!/bin/sh
set -e
MAX_RUNS=${MAX_RUNS:-"-1"}
MONITOR_PATH=${MONITOR_PATH:-"webhdfs/v1/?op=LISTSTATUS"}
OPTIONS=""

if [ "$DROP_USERNAME" = "true" ]; then
  OPTIONS="$OPTIONS -drop-username"
fi

if [ "$DEBUG" = "true" ]; then
  OPTIONS="$OPTIONS -debug"
fi

if [ ! -z "$PROPER_USERNAME" ]; then
  OPTIONS="$OPTIONS -proper-username $PROPER_USERNAME"
fi

if [ "$DISABLE_PAX_FAST" = "true" ]; then
  OPTIONS="$OPTIONS -disable-pax-fast"
fi

if [ "$DEMAND_DELEGATION_TOKEN" = "true" ]; then
  OPTIONS="$OPTIONS -demand-delegation-token"
fi

healthcheck() {
while true; do
  sleep 5;
  curl -o /dev/null -s -q http://${LISTEN_ADDRESS}/${MONITOR_PATH} || wget -O /dev/null -q http://${LISTEN_ADDRESS}/${MONITOR_PATH} || break
done 2> /dev/null
echo "Seppuku"
killall spnego-proxy
kill -9 1
}

RUNS=0
healthcheck &
while true; do
set +e
/spnego-proxy \
  -addr "${LISTEN_ADDRESS}" \
  -metrics-addr "${METRICS_ADDRESS}" \
  -config "${KRB5_CONF}" \
  -user "${KRB5_USER}" \
  -realm "${KRB5_REALM}" \
  -consul-address "${CONSUL_ADDRESS}" \
  -proxy-service "${CONSUL_SERVICE_TO_PROXY}" \
  -spn-service-type "${SPN_SERVICE_TYPE}" \
  -keytab-file "${KRB5_KEYTAB}" \
  $OPTIONS
if [ "$MAX_RUNS" = "$RUNS" ]; then
  exit 999
fi
set -e
RUNS=$(($RUNS + 1))
done
wait
