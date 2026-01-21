FROM public.ecr.aws/amazonlinux/amazonlinux:2023 AS builder

ARG TARGETARCH

RUN dnf -y install tar gzip

# Copy all archives and extract only the correct one
COPY *.tar.gz /tmp/
RUN tar -xzf /tmp/*-linux-${TARGETARCH}.tar.gz -C /tmp && \
    rm -f /tmp/*.tar.gz && \
    mv /tmp/opensearch-data-prepper-* /tmp/data-prepper

FROM public.ecr.aws/amazonlinux/amazonlinux:2023

ARG PIPELINE_FILEPATH
ARG CONFIG_FILEPATH

ENV DATA_PREPPER_PATH=/usr/share/data-prepper
ENV ENV_CONFIG_FILEPATH=$CONFIG_FILEPATH
ENV ENV_PIPELINE_FILEPATH=$PIPELINE_FILEPATH

# Update all packages
RUN dnf -y update
RUN dnf -y install bash bc shadow-utils
RUN dnf -y upgrade

# Create a dedicated user and group with specific UID/GID
RUN useradd -u 1000 -M -U -d / -s /sbin/nologin -c "Data Prepper" data_prepper

# Setup the Adoptium package repo and install Temurin Java
ADD adoptium.repo /etc/yum.repos.d/adoptium.repo
RUN dnf -y install temurin-17-jdk

RUN mkdir -p /var/log/data-prepper

# Copy extracted data-prepper from builder stage
COPY --from=builder /tmp/data-prepper /usr/share/data-prepper

COPY default-data-prepper-config.yaml $ENV_CONFIG_FILEPATH
COPY default-keystore.p12 /usr/share/data-prepper/keystore.p12

RUN chown -R 1000:1000 $DATA_PREPPER_PATH /var/log/data-prepper
USER data_prepper

WORKDIR $DATA_PREPPER_PATH
CMD ["bin/data-prepper"]
