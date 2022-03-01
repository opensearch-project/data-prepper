package org.opensearch.dataprepper.expression;

final class ExceptionOverview extends RuntimeException {

    ExceptionOverview(final String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
