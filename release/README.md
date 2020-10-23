## Description

This repository contains the docker image for odfe-pipes.

## Running a build

To build an image check out the corresponding branch for the version and follow steps

*Both of the below options require odfe-pipes configuration file for successful execution*

### Option #1 - Using docker-compose

1. Create a docker-compose file [example](docker/docker-compose.yml)
2. Make sure to pass arg `CONFIG_FILEPATH` with the complete path where the configuration file is mounted
3. execute docker-compose using `docker-compose up`

### Option #2 - Using docker run

```
# from inside simple-ingest-transformation-utility-pipeline directory
docker build -f release/docker/Dockerfile -t odfe-pipes --build-arg CONFIG_FILEPATH=/usr/share/situp/odfe-pipes.yml
docker run --name odfe-pipes --expose 21890 --read-only -v /home/ec2-user/odfe-pipes.yml:/usr/share/situp/odfe-pipes.yml odfe-pipes

```