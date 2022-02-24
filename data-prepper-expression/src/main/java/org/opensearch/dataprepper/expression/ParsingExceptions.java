/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import java.io.PrintStream;
import java.util.List;

/**
 * @since 1.3
 *
 * Exception thrown by {@link ParseTreeParser} if ANTLR parse emits error events.
 */
public class ParsingExceptions extends Exception {
    private final List<Throwable> causes;

    public ParsingExceptions(final String message, final List<Throwable> causes) {
        super(message);
        this.causes = causes;
    }

    @Override
    public void printStackTrace(final PrintStream s) {
        super.printStackTrace(s);

        if (!causes.isEmpty()) {
            s.println("causes");
            for (final Throwable throwable : causes) {
                throwable.printStackTrace(s);
            }
        }
    }
}
