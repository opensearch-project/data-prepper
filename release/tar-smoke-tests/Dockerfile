FROM ubuntu:latest

RUN mkdir /opt/data-prepper
WORKDIR /opt/data-prepper

COPY /Users/smbayer/code/sbayer55/data-prepper/release/archives/linux/build/distributions/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64.tar.gz /opt/data-prepper/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64.tar.gz
RUN tar -xzf /opt/data-prepper/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64.tar.gz

COPY /Users/smbayer/.config/data-prepper/pipeline.yaml /opt/data-prepper/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64/config/pipelines.yaml
COPY /Users/smbayer/.config/data-prepper/data-prepper-config.yaml /opt/data-prepper/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64/config/data-prepper-config.yaml

ENTRYPOINT /opt/data-prepper/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64/data-prepper-tar-install.sh /opt/data-prepper/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64/config/pipelines.yaml /opt/data-prepper/opensearch-data-prepper-jdk-1.3.0-SNAPSHOT-linux-x64/config/data-prepper-config.yaml
