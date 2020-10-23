# building on CentOS 7 to be in line with
# https://github.com/opendistro-for-elasticsearch/opendistro-build/blob/master/elasticsearch


FROM gradle:jdk14 AS GRADLE_BUILD
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src/
RUN gradle jar --daemon

FROM adoptopenjdk/openjdk14:jre-14.0.1_7-alpine
ARG CONFIG_FILEPATH
ENV ENV_CONFIG_FILEPATH=$CONFIG_FILEPATH
ENV SITUP_PATH /usr/share/situp
RUN mkdir -p $SITUP_PATH
WORKDIR $SITUP_PATH
COPY --from=GRADLE_BUILD /home/gradle/src/situp-core/build/libs/situp*.jar /usr/share/situp/odfe-pipes.jar
CMD java -jar odfe-pipes.jar ${ENV_CONFIG_FILEPATH}

