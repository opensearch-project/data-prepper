FROM gradle:jdk11 as cache
RUN mkdir -p /home/gradle/cache_home
ENV GRADLE_USER_HOME /home/gradle/cache_home
COPY analytics-service/build.gradle /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle clean build --daemon

FROM gradle:jdk11 AS build
COPY --from=cache /home/gradle/cache_home /home/gradle/.gradle
COPY analytics-service /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle bootJar --daemon

RUN wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.0.1/opentelemetry-javaagent-all.jar

FROM openjdk:8-jre-alpine

RUN apk update \
&& apk upgrade \
# add for grpcio
&& apk add --no-cache g++ \
# add for grpcio
&& apk add --no-cache linux-headers \
&& apk add --no-cache bash \
&& apk add --no-cache --virtual=build-dependencies unzip \
&& apk add --no-cache curl \
# add python3-dev for grpcio
&& apk add --no-cache python3 python3-dev

RUN mkdir /app
COPY . /app/
COPY --from=build /home/gradle/src/build/libs/*.jar /app/spring-boot-application.jar
COPY --from=build /home/gradle/src/opentelemetry-javaagent-all.jar /app/opentelemetry-javaagent-all.jar

RUN pip3 install --upgrade pip
RUN pip3 install -r /app/requirements.txt
WORKDIR /app
RUN chmod +x /app/script.sh
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.2.1/wait /app/wait
RUN chmod +x /app/wait

CMD /app/wait && /app/script.sh
