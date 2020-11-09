#!/bin/bash

python3 databaseService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8083) != 000 ]]; do
  echo "Waiting for databaseService to be ready"
  sleep 1
done

python3 orderService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8088) != 000 ]]; do
  echo "Waiting for orderService to be ready"
  sleep 1
done

python3 inventoryService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8082) != 000 ]]; do
  echo "Waiting for inventoryService to be ready"
  sleep 1
done

python3 paymentService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8084) != 000 ]]; do
  echo "Waiting for paymentService to be ready"
  sleep 1
done

python3 recommendationService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8086) != 000 ]]; do
  echo "Waiting for recommendationService to be ready"
  sleep 1
done

python3 authenticationService.py