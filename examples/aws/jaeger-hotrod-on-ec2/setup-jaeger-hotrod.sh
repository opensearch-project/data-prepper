#!/bin/bash
# Script to install and run the Jaeger Hot R.O.D. sample application.
# Installs docker, docker-compose, writes configuration files, then runs 'docker-compose up'

sudo yum install docker -y
sudo usermod -aG docker $USER
sudo systemctl start docker
sudo curl -L "https://github.com/docker/compose/releases/download/1.28.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

cat <<EOT >> otel-collector-config.yml
receivers:
  jaeger:
    protocols:
      grpc:

exporters:
  otlp/2:
    endpoint: localhost:21890
    insecure: true
  logging:

service:
  pipelines:
    traces:
      receivers: [jaeger]
      exporters: [logging, otlp/2]

EOT

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

sudo /usr/local/bin/docker-compose up -d
