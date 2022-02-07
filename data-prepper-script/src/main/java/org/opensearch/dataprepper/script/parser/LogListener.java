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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogListener implements DataPrepperScriptListener {
    private static final Logger LOG = LoggerFactory.getLogger(LogListener.class);

    private int level = 0;

    private String nCopiesOf(final int n, final String str) {
        return String.join("", Collections.nCopies(Math.max(n, 1), str));
    }

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

    private String getTerminalString(final ParserRuleContext ctx) {
        final List<TerminalNode> terminalNodes = getTerminalNodes(ctx);
        return terminalNodes.stream()
                .map(TerminalNode::getText)
                .map(text -> "'" + text + "'")
                .collect(Collectors.joining(", "));
    }

    private String prefix() {
        return String.join("|", Collections.nCopies(level, "\t"));
    }

    private String getOp(final DataPrepperScriptParser.ExpressionContext ctx) {
        try {
            final Field opField = ctx.getClass().getDeclaredField("op");
            opField.setAccessible(true);
            final Object op = opField.get(ctx);

            if (op != null) {
                if (op instanceof Token) {
                    return ((Token) op).getText();
                }
                else {
                    return op.toString();
                }
            }
            else {
                return null;
            }
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            LOG.error("Unable to getOp on {}", ctx.getText(), e);
            return null;
        }
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
    }

    @Override
    public void exitEveryRule(final ParserRuleContext ctx) {
    }
}
