/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.opensearch.dataprepper.script.antlr.DataPrepperScriptListener;
import org.opensearch.dataprepper.script.antlr.DataPrepperScriptParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @since 1.3
 * LogListener is a utility listener that logs every event enter and exit. Useful for debugging and developing new
 * listeners.
 */
public class LogListener implements DataPrepperScriptListener {
    private static final Logger LOG = LoggerFactory.getLogger(LogListener.class);

    private int level = 0;

    /**
     * @since 1.3
     * Utility function to repeat String str, n number of times.
     * @param n is the number of times str will be printed
     * @param str the string to be repeated.
     * @return String containing n instances of str
     */
    private String nCopiesOf(final int n, final String str) {
        return String.join("", Collections.nCopies(Math.max(n, 1), str));
    }

    /**
     * @since 1.3
     * Get self or child nodes of type Terminal Node (Indivisible Rule/Token)
     * @param ctx Current parsing context
     * @return List of self or all child nodes that are Terminal Nodes. List may be empty if no terminal nodes found.
     */
    private List<TerminalNode> getTerminalNodes(final ParserRuleContext ctx) {
        if (ctx instanceof TerminalNode) {
            return Collections.singletonList((TerminalNode) ctx);
        }
        else if (ctx.children == null) {
            return Collections.emptyList();
        }
        else {
            return ctx.children.stream()
                    .flatMap(tree -> {
                        if (tree instanceof TerminalNode) {
                            return Stream.of((TerminalNode) tree);
                        }
                        else if (tree instanceof DataPrepperScriptParser.LiteralContext){
                            return getTerminalNodes((DataPrepperScriptParser.LiteralContext) tree).stream();
                        }
                        else {
                            return Stream.empty();
                        }
                    })
                    .collect(Collectors.toList());
        }
    }

    /**
     * @since 1.3
     * Get terminal nodes a convert to a string of comma separated values
     * @param ctx Current parsing context
     * @return string of comma separated values
     */
    private String getTerminalString(final ParserRuleContext ctx) {
        final List<TerminalNode> terminalNodes = getTerminalNodes(ctx);
        return terminalNodes.stream()
                .map(TerminalNode::getText)
                .map(text -> "'" + text + "'")
                .collect(Collectors.joining(", "));
    }

    /**
     * @since 1.3
     * Creates an indented prefix for pretty format printed hierarchical structures
     * @return String of tabs based on level in hierarchy
     */
    private String prefix() {
        return String.join("|", Collections.nCopies(level, "\t"));
    }

    @Override
    public void enterStatement(final DataPrepperScriptParser.StatementContext ctx) {
        LOG.info("{}enterStatement: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitStatement(final DataPrepperScriptParser.StatementContext ctx) {
        level--;
        LOG.info("{}exitStatement: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterExpression(final DataPrepperScriptParser.ExpressionContext ctx) {
        final String terminals = getTerminalString(ctx);
        LOG.info("{}enterExpression: {} -> Terminal: {}", prefix(), ctx.getText(), terminals);
        level++;
    }

    @Override
    public void exitExpression(final DataPrepperScriptParser.ExpressionContext ctx) {
        final String terminals = getTerminalString(ctx);
        level--;
        LOG.info("{}exitExpression: {} -> Terminal: {}", prefix(), ctx.getText(), terminals);
    }


    @Override
    public void enterPrimary(final DataPrepperScriptParser.PrimaryContext ctx) {
        LOG.info("{}enterPrimary: {}", prefix(), ctx.getText());
        level++;

    }

    @Override
    public void exitPrimary(final DataPrepperScriptParser.PrimaryContext ctx) {
        level--;
        LOG.info("{}exitPrimary: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterRegexPattern(final DataPrepperScriptParser.RegexPatternContext ctx) {
        LOG.info("{}enterRegexPattern: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitRegexPattern(final DataPrepperScriptParser.RegexPatternContext ctx) {
        level--;
        LOG.info("{}exitRegexPattern: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterExpressionInitializer(final DataPrepperScriptParser.ExpressionInitializerContext ctx) {
        LOG.info("{}enterExpressionInitializer: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitExpressionInitializer(final DataPrepperScriptParser.ExpressionInitializerContext ctx) {
        level--;
        LOG.info("{}exitExpressionInitializer: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterListInitializer(final DataPrepperScriptParser.ListInitializerContext ctx) {
        LOG.info("{}enterListInitializer: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitListInitializer(final DataPrepperScriptParser.ListInitializerContext ctx) {
        level--;
        LOG.info("{}exitListInitializer: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterLiteral(final DataPrepperScriptParser.LiteralContext ctx) {
        LOG.info("{}enterLiteral: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitLiteral(final DataPrepperScriptParser.LiteralContext ctx) {
        level--;
        LOG.info("{}exitLiteral: {}", prefix(), ctx.getText());
    }

    @Override
    public void visitTerminal(final TerminalNode node) {
        LOG.info("{}visitTerminal: {}", prefix(), node.getText());
    }

    /**
     * @since 1.3
     * Logs all error nodes visited with indicator where the parsing error occurred in the statement. Trigger on Lexer
     * error only, parser error will be available in context variable on all other listener functions Sample output:
     * <pre>
     * Parsing error expected token '[' at position 5<br>
     * 5 in true<br>
     *      ^
     * </pre>
     * @param node cause of lexer error
     */
    @Override
    public void visitErrorNode(final ErrorNode node) {
        LOG.warn("visitErrorNode: {}", node.getText());

        final Token symbol = node.getSymbol();
        final String sourceStatement = symbol.getInputStream().toString();
        final String locationIdentifier = nCopiesOf(symbol.getCharPositionInLine(), " ") + '^';
        LOG.error(
                "Parsing error {} at position {}\n{}\n{}",
                node.getText(),
                symbol.getCharPositionInLine(),
                sourceStatement,
                locationIdentifier
        );
    }

    @Override
    public void enterEveryRule(final ParserRuleContext ctx) {
        if (ctx.exception != null) {
            LOG.error("Parser exception {} thrown parsing {} on enter rule", ctx.exception, ctx.getText());
        }
    }

    @Override
    public void exitEveryRule(final ParserRuleContext ctx) {
        if (ctx.exception != null) {
            // Log errors will be printed by enterEveryRule
            LOG.trace("Parser exception {} thrown parsing {} on exit rule", ctx.exception, ctx.getText());
        }
    }
}
