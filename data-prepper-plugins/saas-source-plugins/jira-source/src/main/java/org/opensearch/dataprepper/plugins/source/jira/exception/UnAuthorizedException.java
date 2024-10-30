package org.opensearch.dataprepper.plugins.source.jira.exception;

/**
 * Exception to indicate unauthorized access.
 * It could either be caused by invalid credentials supplied by the user or failed renew the credentials.
 */
public final class UnAuthorizedException extends RuntimeException {
    public UnAuthorizedException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public UnAuthorizedException(final String message) {
        super(message);
    }
}
