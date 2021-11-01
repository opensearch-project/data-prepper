package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception for Logtsash converter
 *
 * @since 1.2
 */

public class LogstashConfigurationException extends RuntimeException {

    public LogstashConfigurationException(String errorMessage) {
        super(errorMessage);
    }
}
