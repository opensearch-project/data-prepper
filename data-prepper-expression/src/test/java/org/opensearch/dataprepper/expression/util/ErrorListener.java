/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionBaseListener;

import java.util.BitSet;

/**
 * ErrorListener implements all ANTLR listeners to track if any error event occurred during any phase of parsing
 */
public class ErrorListener extends DataPrepperExpressionBaseListener implements ANTLRErrorListener {
    private boolean errorFound = false;
    private boolean warningFound = false;

    public boolean isErrorFound() {
        return errorFound;
    }

    public boolean isWarningFound() {
        return warningFound;
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
        errorFound = true;
    }

    @Override
    public void reportAmbiguity(
            final Parser recognizer,
            final DFA dfa,
            final int startIndex,
            final int stopIndex,
            final boolean exact,
            final BitSet ambigAlts,
            final ATNConfigSet configs
    ) {
        warningFound = true;
    }

    @Override
    public void reportAttemptingFullContext(
            final Parser recognizer,
            final DFA dfa,
            final int startIndex,
            final int stopIndex,
            final BitSet conflictingAlts,
            final ATNConfigSet configs
    ) {
        warningFound = true;
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
        warningFound = true;
    }

    @Override
    public void visitErrorNode(final ErrorNode node) {
        super.visitErrorNode(node);
        errorFound = true;
    }

    @Override
    public void enterEveryRule(final ParserRuleContext ctx) {
        super.enterEveryRule(ctx);
        if (ctx.exception != null) {
            errorFound = true;
        }
    }
}
