FROM public.ecr.aws/amazonlinux/amazonlinux:2023

ARG PIPELINE_FILEPATH
ARG CONFIG_FILEPATH
ARG ARCHIVE_FILE
ARG ARCHIVE_FILE_UNPACKED

ENV DATA_PREPPER_PATH /usr/share/data-prepper
ENV ENV_CONFIG_FILEPATH=$CONFIG_FILEPATH
ENV ENV_PIPELINE_FILEPATH=$PIPELINE_FILEPATH

# Update all packages
RUN dnf -y update
RUN dnf -y install bash bc
RUN dnf -y upgrade

# Setup the Adoptium package repo and install Temurin Java
ADD adoptium.repo /etc/yum.repos.d/adoptium.repo
RUN dnf -y install temurin-17-jdk

RUN mkdir -p /var/log/data-prepper
ADD $ARCHIVE_FILE /usr/share
RUN mv /usr/share/$ARCHIVE_FILE_UNPACKED /usr/share/data-prepper

COPY default-data-prepper-config.yaml $ENV_CONFIG_FILEPATH
COPY default-keystore.p12 /usr/share/data-prepper/keystore.p12

WORKDIR $DATA_PREPPER_PATH
CMD ["bin/data-prepper"]
