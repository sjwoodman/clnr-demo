#!/bin/bash

[ -z "$INGEST_URL" ] && echo "Need to set INGEST_URL" && exit 1;

echo "Sending data to ${INGEST_URL}"

cat $1 | while read line
do
    if [[ $line != \#* ]] ; then
        IFS=', ' read -r -a array <<< "$line"

        cedate=${array[0]};
        cetime=${array[1]};
        customerId=${array[2]};
        value=${array[3]};
        eventId=$(uuidgen)

        cedate=${cedate//./-}
        cedate=$cedate"T"$cetime".000Z"

        cloudevent="{  \"eventType\": \"com.clnr.meterreading\",  \"eventID\": \"$eventId\",  \"eventTime\": \"$cedate\",  \"eventTypeVersion\": \"2.0\",  \"source\": \"https://example.com\",  \"extensions\": {},  \"contentType\": \"application/json\",  \"cloudEventsVersion\": \"0.1\",  \"data\":  {    \"customerId\" : \"$customerId\",     \"kWh\" : $value  } }"

        echo $cloudevent;

        curl -d "$cloudevent" -H "Content-Type: application/json" -s -o /dev/null -w "%{http_code}\n" -X POST http://${INGEST_URL}/clnr/ce

        sleep 2

    fi
done
