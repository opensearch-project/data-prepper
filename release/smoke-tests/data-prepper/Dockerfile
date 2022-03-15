#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

ARG DOCKER_FILE_DIR
ARG FROM_IMAGE_NAME
ARG FROM_IMAGE_TAG

FROM ${FROM_IMAGE_NAME}:${FROM_IMAGE_TAG}

ARG DATA_PREPPER_VERSION
ARG BUILD_NAME
ARG TAR_FILE
ARG INSTALL_DIR="/opt/data-prepper"

RUN mkdir ${INSTALL_DIR}
WORKDIR ${INSTALL_DIR}

ENV DATA_PREPPER_HOME="${INSTALL_DIR}/${BUILD_NAME}"
ENV CONFIG_DIR="${DATA_PREPPER_HOME}/config"

COPY ${DOCKER_FILE_DIR}/${TAR_FILE} ${INSTALL_DIR}
RUN tar -xzf "${INSTALL_DIR}/${TAR_FILE}" && rm -f "${INSTALL_DIR}/${TAR_FILE}"

COPY ${DOCKER_FILE_DIR}/config/pipelines.yaml ${CONFIG_DIR}
COPY ${DOCKER_FILE_DIR}/config/data-prepper-config.yaml ${CONFIG_DIR}

ENTRYPOINT ${DATA_PREPPER_HOME}/data-prepper-tar-install.sh ${CONFIG_DIR}/pipelines.yaml ${CONFIG_DIR}/data-prepper-config.yaml
