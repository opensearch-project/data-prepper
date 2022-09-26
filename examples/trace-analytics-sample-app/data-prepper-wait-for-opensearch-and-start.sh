#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

until [[ $(curl --write-out %{http_code} --output /dev/null --silent --head --fail https://node-0.example.com:9200 -u admin:admin --insecure) == 200 ]]; do
  echo "Waiting for OpenSearch to be ready"
  sleep 1
done

java -jar data-prepper.jar pipelines.yaml data-prepper-config.yaml
