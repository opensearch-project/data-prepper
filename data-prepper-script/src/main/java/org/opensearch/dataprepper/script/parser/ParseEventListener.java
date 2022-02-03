/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script.parser;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.script.antlr.DataPrepperScriptBaseListener;
import org.opensearch.dataprepper.script.antlr.DataPrepperScriptParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

public class ParseEventListener extends DataPrepperScriptBaseListener {
    private static final Logger LOG = LoggerFactory.getLogger(ParseEventListener.class);

    private static final List<String> operatorTokens = Arrays.asList(
            "[", ",", "]",
            "(", ")",
            "in", "not in",
            "<", "<=", ">", ">=",
            "=~", "!~",
            "==", "!=",
            "and", "or", "not"
    );

    private int stack = 0;

    private final Stack<Token> output = new Stack<>();
    private final Stack<Token> operator = new Stack<>();

    private void popAllOperator() {
        while (!operator.empty()) {
            output.push(operator.pop());
        }
    }

    public void endTree() {
        popAllOperator();
        final String outputString = output.stream().map(Token::getText).collect(Collectors.joining("  "));
        LOG.info("Output = {}", outputString);
    }

    @Override
    public void visitTerminal(final TerminalNode node) {
        final String prefix = String.join("", Collections.nCopies(stack + 1, "\t"));
        LOG.warn("{} {}", prefix, node.getText());
        final Token symbol = node.getSymbol();
        if (operatorTokens.contains(symbol.getText())) {
            if (!operator.empty()) {
                final Token lastOperator = operator.peek();
                if (operatorTokens.indexOf(symbol.getText()) < operatorTokens.indexOf(lastOperator.getText())) {
                    popAllOperator();
                }
            }
            operator.push(symbol);
        }
        else {
            output.push(symbol);
        }
//        current.addValue(node.getSymbol());
    }

    @Override
    public void enterTerm(final DataPrepperScriptParser.TermContext ctx) {
        final String prefix = String.join("", Collections.nCopies(stack, "\t"));
        LOG.info("{} { {}", prefix, ctx.getText());
        stack++;
    }

    @Override
    public void exitTerm(final DataPrepperScriptParser.TermContext ctx) {
        stack--;
        final String prefix = String.join("", Collections.nCopies(stack, "\t"));
        LOG.info("{} } exit: {}", prefix, ctx.getText());
    }
}
