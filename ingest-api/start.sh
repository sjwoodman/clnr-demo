#!/bin/bash

[ -z "$PRODUCER_URL" ] && echo "Need to set PRODUCER_URL" && exit 1;

echo "Sending data to ${PRODUCER_URL}"
cat clnr.txt | while read line
do
    if [[ $line != \#* ]] ; then

        echo $line;
        curl -d "$line" -H "Content-Type: text/plain" -s -o /dev/null -w "%{http_code}\n" -X POST http://${PRODUCER_URL}/clnr/reading/csv
        sleep 0.2

    fi
done