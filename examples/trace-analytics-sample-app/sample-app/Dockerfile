FROM gradle:jdk17 as cache
RUN mkdir -p /home/gradle/cache_home
ENV GRADLE_USER_HOME /home/gradle/cache_home
COPY analytics-service/build.gradle /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle clean build --daemon

FROM gradle:jdk17 AS build
COPY --from=cache /home/gradle/cache_home /home/gradle/.gradle
COPY analytics-service /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle bootJar --daemon

RUN wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.30.0/opentelemetry-javaagent.jar

FROM eclipse-temurin:17-jre-jammy

RUN apt-get -y update \
&& apt-get -y upgrade \
# add for grpcio
&& apt-get -y install g++ \
# add for grpcio
&& apt-get -y install linux-generic \
&& apt-get -y install bash \
&& apt-get -y install unzip \
&& apt-get -y install curl \
# add python3-dev for grpcio
&& apt-get -y install python3 python3-pip python3-dev

RUN mkdir /app
COPY . /app/
COPY --from=build /home/gradle/src/build/libs/*.jar /app/spring-boot-application.jar
COPY --from=build /home/gradle/src/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar

RUN pip3 install --upgrade pip
RUN pip3 install -r /app/requirements.txt
WORKDIR /app
RUN chmod +x /app/script.sh
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.2.1/wait /app/wait
RUN chmod +x /app/wait

CMD /app/wait && /app/script.sh
