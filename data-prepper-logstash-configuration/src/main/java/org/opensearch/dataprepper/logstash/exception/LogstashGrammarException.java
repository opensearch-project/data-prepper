package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception thrown when ANTLR fails to parse the Logstash configuration
 *
 * @since 1.2
 */
public class LogstashGrammarException extends LogstashParsingException {

    public LogstashGrammarException(String errorMessage) {
        super(errorMessage);
    }
}