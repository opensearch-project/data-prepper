FROM gradle:jdk14 AS builder
COPY . /home/gradle/src
WORKDIR /home/gradle/src/data-prepper-core
RUN gradle clean jar --daemon

FROM amazoncorretto:15-al2-full
EXPOSE 21890
WORKDIR /usr/share/data-prepper/
COPY --from=builder /home/gradle/src/data-prepper-core/build/libs/data-prepper*.jar /usr/share/data-prepper/data-prepper.jar
CMD ["java", "-Xms128m", "-Xmx128m", "-jar", "data-prepper.jar", "pipelines.yaml", "data-prepper-config.yaml"]