# building on CentOS 7 to be in line with
# https://github.com/opendistro-for-elasticsearch/opendistro-build/blob/master/elasticsearch

FROM adoptopenjdk/openjdk14:jre-14.0.1_7-alpine
ARG CONFIG_FILEPATH
ARG JAR_FILE
ENV ENV_CONFIG_FILEPATH=$CONFIG_FILEPATH
ENV SITUP_PATH /usr/share/situp
RUN mkdir -p $SITUP_PATH
COPY $JAR_FILE /usr/share/situp/odfe-pipes.jar
WORKDIR $SITUP_PATH
CMD java $JAVA_OPTS -jar odfe-pipes.jar ${ENV_CONFIG_FILEPATH}

