#!/bin/bash

until [[ $(curl --write-out %{http_code} --output /dev/null --silent --head --fail http://elastic:9200) == 200 ]]; do
  echo "Waiting for Elastic OSS to be ready"
  sleep 1
done

exec java -jar situp.jar /app/transformation-instance.yml