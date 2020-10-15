FROM gradle:jdk14 AS builder
COPY . /home/gradle/src
WORKDIR /home/gradle/src/situp-core
RUN gradle jar --daemon

FROM adoptopenjdk/openjdk14:jre-14.0.1_7-alpine
EXPOSE 21890
WORKDIR /app
COPY --from=builder /home/gradle/src/situp-core/build/libs/situp*.jar /app/situp.jar
ADD /examples/trace_analytics.yml /app/transformation-instance.yml
ADD /examples/demo/root-ca.pem    /app/root-ca.pem
CMD ["java", "-jar", "situp.jar", "/app/transformation-instance.yml"]