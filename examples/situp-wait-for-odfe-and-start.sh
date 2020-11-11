#!/bin/bash

until [[ $(curl --write-out %{http_code} --output /dev/null --silent --head --fail https://node-0.example.com:9200 -u admin:admin --insecure) == 200 ]]; do
  echo "Waiting for ODFE to be ready"
  sleep 1
done

java -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=8849 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.rmi.port=8849 -Djava.rmi.server.hostname=localhost -jar situp.jar /app/transformation-instance.yml