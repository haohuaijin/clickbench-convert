#!/bin/bash

TRIES=3
QUERY_NUM=1
echo $1
cat queries.sql | while read -r query; do
    sync
    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(openobserve sql -e "$query" -t 1y 2>&1 | grep "took:" | awk '{ print $(NF-1) }')
        [[ $RES != "" ]] && \
            echo -n "$RES" || \
            echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
        echo "${QUERY_NUM},${i},${RES}" >> result.csv
    done
    echo "],"

    QUERY_NUM=$((QUERY_NUM + 1))
done
