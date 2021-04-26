FROM gradle:jdk14 AS builder
COPY . /home/gradle/src
WORKDIR /home/gradle/src
# TODO: replace local built OpenSearch artifact with the public artifact
RUN git clone https://github.com/opensearch-project/OpenSearch.git && cd OpenSearch && git checkout 1.0.0-alpha2 -b alpha2-release \
&& ./gradlew publishToMavenLocal -Dbuild.version_qualifier=alpha2 -Dbuild.snapshot=false
WORKDIR /home/gradle/src/data-prepper-core
RUN gradle clean jar --daemon

FROM amazoncorretto:15-al2-full
EXPOSE 21890
WORKDIR /usr/share/data-prepper/
COPY --from=builder /home/gradle/src/data-prepper-core/build/libs/data-prepper*.jar /usr/share/data-prepper/data-prepper.jar
CMD ["java", "-Xms128m", "-Xmx128m", "-jar", "data-prepper.jar", "pipelines.yaml", "data-prepper-config.yaml"]