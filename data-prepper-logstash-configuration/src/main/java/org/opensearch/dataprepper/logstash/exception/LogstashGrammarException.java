package org.opensearch.dataprepper.logstash.exception;

public class LogstashGrammarException extends LogstashParsingException {

    public LogstashGrammarException(String errorMessage) {
        super(errorMessage);
    }
}