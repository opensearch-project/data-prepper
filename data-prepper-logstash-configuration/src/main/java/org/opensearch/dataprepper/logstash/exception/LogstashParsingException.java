package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception for visitor when it's unable to convert into Logstash models
 *
 * @since 1.2
 */

public class LogstashParsingException extends LogstashConfigurationException {

    public LogstashParsingException(String errorMessage) {
        super(errorMessage);
    }
}