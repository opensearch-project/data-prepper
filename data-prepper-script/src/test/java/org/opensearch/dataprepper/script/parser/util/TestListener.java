/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script.parser.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.script.antlr.DataPrepperScriptParser;
import org.opensearch.dataprepper.script.parser.LogListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

public class TestListener extends LogListener {
    private static final Logger LOG = LoggerFactory.getLogger(TestListener.class);

    private static final String ESCAPED_FORWARD_SLASH = "|escaped-forward-slash|";
    private static final String ESCAPED_DOUBLE_QUOTE = "|escaped-double-quote|";

    private static final List<Integer> PARENTHESIS_SYMBOL_TYPES = Arrays.asList(
            DataPrepperScriptParser.LPAREN,
            DataPrepperScriptParser.RPAREN
    );
    private static final List<Integer> KEY_SYMBOL_TYPES = Arrays.asList(
            DataPrepperScriptParser.EQUAL,
            DataPrepperScriptParser.NOT_EQUAL,
            DataPrepperScriptParser.LT,
            DataPrepperScriptParser.LTE,
            DataPrepperScriptParser.GT,
            DataPrepperScriptParser.GTE,
            DataPrepperScriptParser.MATCH_REGEX_PATTERN,
            DataPrepperScriptParser.NOT_MATCH_REGEX_PATTERN,
            DataPrepperScriptParser.IN_LIST,
            DataPrepperScriptParser.NOT_IN_LIST,
            DataPrepperScriptParser.AND,
            DataPrepperScriptParser.OR,
            DataPrepperScriptParser.NOT,
            DataPrepperScriptParser.LBRACK,
            DataPrepperScriptParser.RBRACK,
            DataPrepperScriptParser.TRUE,
            DataPrepperScriptParser.FALSE,
            DataPrepperScriptParser.FORWARDSLASH,
            DataPrepperScriptParser.DOUBLEQUOTE,
            DataPrepperScriptParser.LISTSEPARATOR,
            DataPrepperScriptParser.SPACE,
            DataPrepperScriptParser.OTHER
    );

    private final ObjectMapper mapper = new ObjectMapper();
    private final ArrayNode statementArray = mapper.createArrayNode();
    private final List<ErrorNode> errorNodeList = new LinkedList<>();
    private final List<Exception> exceptionList = new LinkedList<>();
    private final Stack<ArrayNode> stack = new Stack<>();

    public TestListener() {
    }

    @Override
    public void visitTerminal(final TerminalNode node) {
        super.visitTerminal(node);

        if (node.getSymbol().getType() == DataPrepperScriptParser.Integer) {
            final Integer terminal = Integer.parseInt(node.getSymbol().getText());
            stack.peek().add(terminal);
        }
        else if (node.getSymbol().getType() == DataPrepperScriptParser.Float) {
            final Float terminal = Float.parseFloat(node.getSymbol().getText());
            stack.peek().add(terminal);
        }
        else if (node.getSymbol().getType() == DataPrepperScriptParser.Boolean) {
            final Boolean terminal = Boolean.parseBoolean(node.getSymbol().getText());
            stack.peek().add(terminal);
        }
        else if (node.getSymbol().getType() == DataPrepperScriptParser.JsonPointer) {
            String jsonPointer = node.getSymbol().getText();
            if (jsonPointer.startsWith("\"")) {
                // Remove surrounding " on json pointer
                jsonPointer = jsonPointer.substring(1, jsonPointer.length() - 1);
                // Handle escape sequence that jackson will escape incorrectly, triggering a recursive escape sequence
                jsonPointer = jsonPointer.replace("\\/", ESCAPED_FORWARD_SLASH);
                // Remove \ characters, Jackson will duplicate them recursively
                jsonPointer = jsonPointer.replace("\\", "");
            }
            stack.peek().add(jsonPointer);
        }
        else if (node.getSymbol().getType() == DataPrepperScriptParser.String) {
            String stringNode = node.getSymbol().getText();
            // Remove surrounding " on strings
            stringNode = stringNode.substring(1, stringNode.length() - 1);
            // Remove \ characters, Jackson will duplicate them recursively
            stringNode = stringNode.replace("\\", "");
            stack.peek().add(stringNode);
        }
        else if (KEY_SYMBOL_TYPES.contains(node.getSymbol().getType())) {
            stack.peek().add(node.getSymbol().getText());
        }
        else if (PARENTHESIS_SYMBOL_TYPES.contains(node.getSymbol().getType())) {
            LOG.debug("Token {} not added to statement array", node.getSymbol().getText());
        }
        else if (node.getSymbol().getType() == DataPrepperScriptParser.EOF) {
            LOG.debug("End of statement reached");
        }
        else {
            LOG.error("Unknown symbol type {} for node \"{}\"", node.getSymbol().getType(), node.getSymbol().getText());
        }
    }

    private void enterNode(final ParserRuleContext ctx) {
        if (ctx.exception != null) {
            LOG.warn("Parse Exception {} found on \"{}\"", ctx.exception, ctx.getText());
            exceptionList.add(ctx.exception);
        }
        if (stack.empty()) {
            if (!statementArray.isEmpty()) {
                LOG.error("Stack unexpectedly empty, possible reused listener?");
                LOG.warn("Clearing statement array {}", statementArray.toPrettyString());
                statementArray.removeAll();
                errorNodeList.clear();
            }
            stack.push(statementArray);
        }
        else {
            final ArrayNode expr = stack.peek().addArray();
            stack.push(expr);
        }
    }

    private void exitNode(final ParserRuleContext ctx) {
        stack.pop();
    }

    @Override
    public void enterStatement(final DataPrepperScriptParser.StatementContext ctx) {
        super.enterStatement(ctx);
//        enterNode(ctx);
    }

    @Override
    public void exitStatement(final DataPrepperScriptParser.StatementContext ctx) {
        super.exitStatement(ctx);
//        exitNode(ctx);
    }

    @Override
    public void enterExpression(final DataPrepperScriptParser.ExpressionContext ctx) {
        super.enterExpression(ctx);
        enterNode(ctx);
    }

    @Override
    public void exitExpression(final DataPrepperScriptParser.ExpressionContext ctx) {
        super.exitExpression(ctx);
        exitNode(ctx);
    }

    @Override
    public void enterListInitializer(final DataPrepperScriptParser.ListInitializerContext ctx) {
        super.enterListInitializer(ctx);
        enterNode(ctx);
    }

    @Override
    public void exitListInitializer(final DataPrepperScriptParser.ListInitializerContext ctx) {
        super.exitListInitializer(ctx);
        exitNode(ctx);
    }

    @Override
    public void enterRegexPattern(final DataPrepperScriptParser.RegexPatternContext ctx) {
        super.enterRegexPattern(ctx);
        enterNode(ctx);
    }

    @Override
    public void exitRegexPattern(final DataPrepperScriptParser.RegexPatternContext ctx) {
        super.exitRegexPattern(ctx);
        exitNode(ctx);
    }

    @Override
    public void visitErrorNode(final ErrorNode node) {
        super.visitErrorNode(node);
        errorNodeList.add(node);
    }

    /**
     * Formats JSON array to a less escaped syntax for testing. Strings will be surrounded by ' character instead of ".
     * Examples: "hello" -> 'hello'. "hello \"world\"" -> 'hello \"world\"'. An assertion statement for hello world
     * would be assertThat(helloWorldVal, is("'hello \\\"world\\\"'")).
     * @return formatted json string
     */
    @Override
    public String toString() {
        return statementArray.toString()
                .replace("\\\"", ESCAPED_DOUBLE_QUOTE)
                .replace("\"", "'")
                .replace(ESCAPED_DOUBLE_QUOTE, "\\\"")
                .replace(ESCAPED_FORWARD_SLASH, "\\/");
    }

    public List<ErrorNode> getErrorNodeList() {
        return errorNodeList;
    }

    public List<Exception> getExceptionList() {
        return exceptionList;
    }

    public String toPrettyString() {
        return statementArray.toPrettyString();
    }
}
