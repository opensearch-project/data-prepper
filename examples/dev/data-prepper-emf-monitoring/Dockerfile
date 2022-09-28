FROM gradle:jdk11 AS builder
ARG DATA_PREPPER_VERSION
COPY . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle clean assemble --no-daemon

FROM eclipse-temurin:17-jdk-alpine
ARG DATA_PREPPER_VERSION
EXPOSE 21890

RUN apk update
RUN apk add --no-cache bash bc curl

COPY --from=builder \
    /home/gradle/src/release/archives/linux/build/install/opensearch-data-prepper-${DATA_PREPPER_VERSION}-linux-x64/ \
    /usr/share/data-prepper/
WORKDIR /usr/share/data-prepper/
CMD ./bin/data-prepper
