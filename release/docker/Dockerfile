FROM public.ecr.aws/amazonlinux/amazonlinux:2023 AS builder

ARG TARGETARCH

RUN dnf -y install tar gzip && dnf clean all

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

# Create a dedicated user and group with specific UID/GID
RUN dnf -y install shadow-utils && \
    useradd -u 1000 -M -U -d / -s /sbin/nologin -c "Data Prepper" data_prepper && \
    dnf -y remove shadow-utils && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Setup the Adoptium package repo and install Temurin Java
ADD adoptium.repo /etc/yum.repos.d/adoptium.repo
RUN dnf -y upgrade && \
    dnf -y install temurin-17-jdk bc && \
    dnf clean all && \
    rm -rf /var/cache/dnf

RUN mkdir -p /var/log/data-prepper && \
    chown 1000:1000 /var/log/data-prepper

# Copy extracted data-prepper from builder stage
COPY --from=builder --chown=1000:1000 /tmp/data-prepper /usr/share/data-prepper

COPY --chown=1000:1000 default-data-prepper-config.yaml $ENV_CONFIG_FILEPATH
COPY --chown=1000:1000 default-keystore.p12 /usr/share/data-prepper/keystore.p12

USER data_prepper

WORKDIR $DATA_PREPPER_PATH
CMD ["bin/data-prepper"]
