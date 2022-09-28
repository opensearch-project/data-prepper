FROM gradle:jdk11 as cache
RUN mkdir -p /home/gradle/cache_home
ENV GRADLE_USER_HOME /home/gradle/cache_home
COPY build.gradle /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle clean build --daemon

FROM gradle:jdk11 AS build
COPY --from=cache /home/gradle/cache_home /home/gradle/.gradle
COPY . /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle bootJar --daemon

RUN wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.0.1/opentelemetry-javaagent-all.jar

FROM openjdk:8-jdk-alpine
EXPOSE 8087
RUN mkdir /app
COPY --from=build /home/gradle/src/build/libs/*.jar /app/spring-boot-application.jar
COPY --from=build /home/gradle/src/opentelemetry-javaagent-all.jar /app/opentelemetry-javaagent-all.jar
ENTRYPOINT ["java","-javaagent:/app/opentelemetry-javaagent-all.jar","-jar","/app/spring-boot-application.jar"]
