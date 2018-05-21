#!/bin/bash

[ -z "$INGEST_URL" ] && echo "Need to set INGEST_URL" && exit 1;

echo "Sending data to ${INGEST_URL}"

cat $1 | while read line
do
    if [[ $line != \#* ]] ; then

        echo $line;
        curl -d "$line" -H "Content-Type: text/plain" -s -o /dev/null -w "%{http_code}\n" -X POST http://${INGEST_URL}/clnr/reading/csv
        sleep 0.2

    fi
done