package org.opensearch.dataprepper.logstash.exception;

public class LogstashConfigurationException extends RuntimeException {

    public LogstashConfigurationException(String errorMessage) {
        super(errorMessage);
    }
}
