FROM gradle:jdk11 AS builder
ARG DATA_PREPPER_VERSION
COPY . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle clean :release:archives:buildArchives -Prelease --no-daemon
WORKDIR /home/gradle/src/release/archives/linux/build/distributions
RUN tar -xf opensearch-data-prepper-${DATA_PREPPER_VERSION}-linux-x64.tar.gz

FROM eclipse-temurin:17-jdk-alpine
ARG DATA_PREPPER_VERSION

RUN apk update
RUN apk add --no-cache bash bc curl

COPY --from=builder \
    /home/gradle/src/release/archives/linux/build/distributions/opensearch-data-prepper-${DATA_PREPPER_VERSION}-linux-x64/ \
    /usr/share/data-prepper/
WORKDIR /usr/share/data-prepper/
CMD ./bin/data-prepper pipelines.yaml data-prepper-config.yaml