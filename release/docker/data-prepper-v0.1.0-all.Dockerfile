# building on CentOS 7 to be in line with
# https://github.com/opendistro-for-elasticsearch/opendistro-build/blob/master/elasticsearch

FROM amazoncorretto:15-al2-full
ARG CONFIG_FILEPATH
ARG JAR_FILE
ENV ENV_CONFIG_FILEPATH=$CONFIG_FILEPATH
ENV DATA_PREPPER_PATH /usr/share/data-prepper
RUN mkdir -p $DATA_PREPPER_PATH
COPY $JAR_FILE /usr/share/data-prepper/data-prepper.jar
WORKDIR $DATA_PREPPER_PATH
CMD java $JAVA_OPTS -jar data-prepper.jar ${ENV_CONFIG_FILEPATH}

