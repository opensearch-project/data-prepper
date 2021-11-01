package org.opensearch.dataprepper.logstash.exception;

/**
 * Exception for ANTLR failures
 *
 * @since 1.2
 */

public class LogstashGrammarException extends LogstashParsingException {

    public LogstashGrammarException(String errorMessage) {
        super(errorMessage);
    }
}