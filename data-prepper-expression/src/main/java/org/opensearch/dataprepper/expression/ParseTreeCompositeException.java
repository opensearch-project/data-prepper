/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @since 1.3
 * <p>
 * Exception thrown by {@link ParseTreeParser} if ANTLR parse emits error events.
 */
class ParseTreeCompositeException extends RuntimeException {
    private static final String SEPARATOR = System.getProperty("line.separator");

    private final Set<Throwable> exceptions;

    public ParseTreeCompositeException(final List<Throwable> exceptions) {
        if (exceptions.isEmpty()) {
            throw new IllegalArgumentException("exceptions is empty");
        }

        this.exceptions = exceptions.stream()
                .map(this::mapNullToNullPointer)
                .collect(Collectors.toSet());
    }

    private Throwable mapNullToNullPointer(@Nullable final Throwable throwable) {
        if (throwable == null) {
            return new NullPointerException("Throwable was null!");
        }
        else {
            return throwable;
        }
    }

    private String throwableListToString(final Set<Throwable> exceptions) {
        final StringBuilder aggregateMessage = new StringBuilder();
        aggregateMessage.append("Multiple exceptions (")
                .append(exceptions.size())
                .append(")")
                .append(SEPARATOR);

        for (final Throwable inner : exceptions) {
            aggregateMessage.append("|-- ");
            aggregateMessage.append(inner.getClass().getCanonicalName()).append(": ");
            aggregateMessage.append(inner.getMessage());

            final StackTraceElement[] stackTrace = inner.getStackTrace();
            if (stackTrace.length > 0) {
                aggregateMessage.append(SEPARATOR)
                        .append("    at ")
                        .append(stackTrace[0])
                        .append(SEPARATOR);
            }
        }
        return aggregateMessage.toString().trim();
    }

    @Override
    public synchronized Throwable getCause() {
        if (exceptions.size() > 1) {
            final String message = throwableListToString(exceptions);
            return new ExceptionOverview(message);
        }
        else {
            return exceptions.iterator().next();
        }
    }
}
