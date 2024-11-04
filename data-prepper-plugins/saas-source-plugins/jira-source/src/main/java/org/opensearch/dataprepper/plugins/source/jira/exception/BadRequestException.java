package org.opensearch.dataprepper.plugins.source.jira.exception;

/**
 * Exception to indicate a bad REST call has been made.
 * It could either be caused by bad user inputs or wrong url construction in the logic.
 */
public final class BadRequestException extends RuntimeException {
    public BadRequestException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public BadRequestException(final String message) {
        super(message);
    }
}
