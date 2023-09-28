FROM gradle:jdk17 as cache
RUN mkdir -p /home/gradle/cache_home
ENV GRADLE_USER_HOME /home/gradle/cache_home
COPY build.gradle /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle clean build --daemon

FROM gradle:jdk17 AS build
COPY --from=cache /home/gradle/cache_home /home/gradle/.gradle
COPY . /home/gradle/src/
WORKDIR /home/gradle/src
RUN gradle bootJar --daemon

RUN wget https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v1.30.0/opentelemetry-javaagent.jar

FROM eclipse-temurin:17-jre-jammy
EXPOSE 8087
RUN mkdir /app
COPY --from=build /home/gradle/src/build/libs/*.jar /app/spring-boot-application.jar
COPY --from=build /home/gradle/src/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar
ENTRYPOINT ["java","-javaagent:/app/opentelemetry-javaagent.jar","-jar","/app/spring-boot-application.jar"]
