FROM gradle:jdk14 AS builder
COPY . /home/gradle/src
WORKDIR /home/gradle/src/situp-core
RUN gradle cleanjar --daemon

FROM adoptopenjdk/openjdk14:jre-14.0.1_7-alpine
EXPOSE 21890
WORKDIR /app
RUN apk --no-cache add curl
COPY --from=builder /home/gradle/src/situp-core/build/libs/situp*.jar /app/situp.jar
CMD ["java", "-Xms128m", "-Xmx128m", "-jar", "situp.jar", "/app/transformation-instance.yml"]