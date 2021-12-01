#!/bin/bash

OPENSEARCH_HOST="localhost:9200"
OPENSEARCH_INDEX="test-grok-index"

docker-compose up --detach

#curl -k -u 'admin:admin' 'https://localhost:9200/_cat/indices'
#curl -k -u 'admin:admin' 'https://localhost:9200/_search?q=apple'
#curl -s -k -u 'admin:admin' 'https://localhost:9200/test-grok-index/_stats' | jq
#curl -s -k -u 'admin:admin' 'https://localhost:9200/test-grok-index/_search' | jq '.hits.total.value'

#curl -k -H "Content-Type: application/json" -d '[{"log": "smoke test log "}]' 'https://localhost:2021/log/ingest'

WAITING_FOR_OPENSEARCH=true

while ${WAITING_FOR_OPENSEARCH}
do
    if curl -s -k -u 'admin:admin' 'https://localhost:9200/_cat/indices' > /dev/null && curl -s -k -H "Content-Type: application/json" -d '[{"log": "smoke test log "}]' 'http://localhost:2021/log/ingest' > /dev/null
    then
        WAITING_FOR_OPENSEARCH=false
    else
        echo "Waiting for opensearch to start"
    fi
    sleep 1s
done

echo "Opensearch started!"

SEARCH_RESPONSE=$(curl -s -k -u 'admin:admin' "https://${OPENSEARCH_HOST}/${OPENSEARCH_INDEX}/_search")

if command -v jq &> /dev/null
then
    LOG_COUNT=$(jq '.hits.total.value' <<< "${SEARCH_RESPONSE}")
else
    LOG_COUNT=$(docker run alpine /bin/sh -c "apk -q --no-cache add jq && echo '${SEARCH_RESPONSE}' | jq '.hits.total.value'")
fi

if [ $LOG_COUNT -gt 0 ]
then
    echo "Open Search is receiving logs from Data Prepper"
    echo "Open Search has received at least ${LOG_COUNT} logs"
else
    echo "No logs found in opensearch node ${OPENSEARCH_HOST} for index ${OPENSEARCH_INDEX}"
fi

#COMPOSE_FILE="docker-compose.yml"
#if test -f ${COMPOSE_FILE}; then
#    rm -f ${COMPOSE_FILE}
#fi
#
#cat << EOF > ${COMPOSE_FILE}
#version: "3.7"
#services:
#  data-prepper:
#    restart: unless-stopped
#    container_name: data-prepper
#    image: opensearchproject/data-prepper:latest
#    volumes:
#      - ../trace_analytics_no_ssl.yml:/usr/share/data-prepper/pipelines.yaml
#      - ../data-prepper-config.yaml:/usr/share/data-prepper/data-prepper-config.yaml
#      - ../demo/root-ca.pem:/usr/share/data-prepper/root-ca.pem
#    ports:
#      - "21890:21890"
#    networks:
#      - my_network
#    depends_on:
#      - "opensearch"
#EOF
