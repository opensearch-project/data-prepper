/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @since 1.3
 *
 * Handles any syntaxError events that occur during parsing. All exceptions are tracked and available after parsing is complete.
 */
class ParserErrorListener implements ANTLRErrorListener {
    private final List<Throwable> exceptions = new ArrayList<>();

    /**
     * @since 1.3
     *
     * Get if any exception events were received
     * @return if exceptions received
     */
    public boolean isErrorFound() {
        return !exceptions.isEmpty();
    }

    /**
     * @since 1.3
     *
     * Clears any error events received. Should be called before a new statement is parsed.
     */
    public void resetErrors() {
        exceptions.clear();
    }

    /**
     * @since 1.3
     *
     * Get a list of all exception events received. List is emptied when {@link ParserErrorListener#resetErrors()}
     *
     * @return list of all exception events received
     */
    public List<Throwable> getExceptions() {
        return exceptions;
    }

    @Override
    public void syntaxError(
            final Recognizer<?, ?> recognizer,
            final Object offendingSymbol,
            final int line,
            final int charPositionInLine,
            final String msg,
            final RecognitionException e
    ) {
        exceptions.add(e);
    }

    @Override
    public void reportAmbiguity(
            final org.antlr.v4.runtime.Parser recognizer,
            final DFA dfa,
            final int startIndex,
            final int stopIndex,
            final boolean exact,
            final BitSet ambigAlts,
            final ATNConfigSet configs
    ) {
    }

    @Override
    public void reportAttemptingFullContext(
            final org.antlr.v4.runtime.Parser recognizer,
            final DFA dfa,
            final int startIndex,
            final int stopIndex,
            final BitSet conflictingAlts,
            final ATNConfigSet configs
    ) {
    }

    @Override
    public void reportContextSensitivity(
            final Parser recognizer,
            final DFA dfa,
            final int startIndex,
            final int stopIndex,
            final int prediction,
            final ATNConfigSet configs
    ) {
    }
}
