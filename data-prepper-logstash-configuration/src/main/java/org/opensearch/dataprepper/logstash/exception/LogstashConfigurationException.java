package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception for Logstash configuration converter
 *
 * @since 1.2
 */
public class LogstashConfigurationException extends RuntimeException {

    public LogstashConfigurationException(String errorMessage) {
        super(errorMessage);
    }
}
