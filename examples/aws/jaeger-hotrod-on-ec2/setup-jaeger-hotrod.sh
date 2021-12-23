#!/bin/bash
#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

# Script to install and run the Jaeger Hot R.O.D. sample application.
# Installs docker, docker-compose, writes configuration files, then runs 'docker-compose up'

# Install Docker
sudo yum install docker -y

# Remove need for sudo when running docker commands
sudo usermod -aG docker $USER

# Start the docker dameon
sudo systemctl start docker

# Download docker-compose 1.28.2 and make it executable
sudo curl -L "https://github.com/docker/compose/releases/download/1.28.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Write an OpenTelemetry Collector configuration file to receive from Jaeger and export to Data Prepper
# See more documentation at https://opentelemetry.io/docs/collector/configuration/
cat <<EOT >> otel-collector-config.yml
receivers:
  jaeger:
    protocols:
      grpc:

processors:
  batch/traces:
    timeout: 1s
    send_batch_size: 50

exporters:
  otlp/data-prepper:
    endpoint: localhost:21890
    insecure: true
  logging:

service:
  pipelines:
    traces:
      receivers: [jaeger]
      exporters: [logging, otlp/data-prepper]

EOT

# Write a Docker Compose file necessary to stand up the HotROD application, a Jaeger agent,
# and an OpenTelemetry Collector.
# See more HotROD documentation at https://github.com/jaegertracing/jaeger/tree/master/examples/hotrod
cat <<EOT >> docker-compose.yml
version: "3.7"
services:
  otel-collector:
    container_name: otel-collector
    network_mode: host
    image: otel/opentelemetry-collector:0.14.0
    command: [ "--config=/etc/otel-collector-config.yml" ]
    volumes:
      - ./otel-collector-config.yml:/etc/otel-collector-config.yml
  jaeger-agent:
    container_name: jaeger-agent
    network_mode: host
    image: jaegertracing/jaeger-agent:latest
    command: [ "--reporter.grpc.host-port=localhost:14250" ]
  jaeger-hot-rod:
    container_name: jaeger-hotrod
    image: jaegertracing/example-hotrod:latest
    network_mode: host
    command: [ "all" ]
    environment:
      - JAEGER_AGENT_HOST=localhost
      - JAEGER_AGENT_PORT=6831
    depends_on:
      - jaeger-agent

EOT

# Run Docker compose to start the HotROD application
# The application can be shut down and cleaned up later by running `sudo /usr/local/bin/docker-compose down`
sudo /usr/local/bin/docker-compose up -d
