#!/bin/bash

until [[ $(curl --write-out %{http_code} --output /dev/null --silent --head --fail https://node-0.example.com:9200 -u admin:admin --insecure) == 200 ]]; do
  echo "Waiting for ODFE to be ready"
  sleep 1
done

java -Xms128m -Xmx128m -jar data-prepper.jar /app/transformation-instance.yml