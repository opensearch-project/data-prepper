package org.opensearch.dataprepper.logstash.exception;

public class LogstashParsingException extends LogstashConfigurationException {

    public LogstashParsingException(String errorMessage) {
        super(errorMessage);
    }
}