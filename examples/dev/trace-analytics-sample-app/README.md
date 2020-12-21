# Dev Trace Analytics Sample App
This directory contains the same sample app under the /examples directory, except with the following changes:
1. Data Prepper is built using the code in this repository, instead of a pulled image from DockerHub
2. A container for dnsmasq is included for network tests

It should be started with `docker-compose up -d --build`, 
 