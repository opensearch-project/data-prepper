package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception thrown when {@link org.opensearch.dataprepper.logstash.parser.LogstashVisitor} is unable to convert
 *  * Logstash configuration into Logstash model objects
 *
 * @since 1.2
 */
public class LogstashParsingException extends LogstashConfigurationException {

    public LogstashParsingException(String errorMessage) {
        super(errorMessage);
    }
}