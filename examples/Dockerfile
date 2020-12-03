FROM gradle:jdk14 AS builder
COPY . /home/gradle/src
WORKDIR /home/gradle/src/data-prepper-core
RUN gradle clean jar --daemon

FROM adoptopenjdk/openjdk14:jre-14.0.1_7-alpine
EXPOSE 21890
WORKDIR /app
RUN apk --no-cache add curl
COPY --from=builder /home/gradle/src/data-prepper-core/build/libs/data-prepper*.jar /app/data-prepper.jar
CMD ["java", "-Xms128m", "-Xmx128m", "-jar", "data-prepper.jar", "/app/transformation-instance.yml"]