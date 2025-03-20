#!/bin/sh
set -x -e
MAX_RUNS=${MAX_RUNS:-"-1"}
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

RUNS=0
while true; do
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
RUNS=$(($RUNS + 1))
done
