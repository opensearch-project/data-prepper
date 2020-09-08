package com.amazon.situp.parser;

public class ParseException extends RuntimeException {
    public ParseException(final Throwable cause) {
        super(cause);
    }

    public ParseException(final String message) {
        super(message);
    }

    public ParseException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
