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

    private List<TerminalNode> getTerminalNodes(final ParserRuleContext ctx) {
        if (ctx instanceof TerminalNode) {
            return Collections.singletonList((TerminalNode) ctx);
        }
        else {
            return ctx.children.stream()
                    .flatMap(tree -> {
                        if (tree instanceof TerminalNode) {
                            return Stream.of((TerminalNode) tree);
                        }
                        else if (tree instanceof DataPrepperScriptParser.TermContext){
                            return getTerminalNodes((DataPrepperScriptParser.TermContext) tree).stream();
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

    private String getOp(final DataPrepperScriptParser.ExprContext ctx) {
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
    public void enterExpr(final DataPrepperScriptParser.ExprContext ctx) {
        final String terminals = getTerminalString(ctx);

        LOG.info("{}enterExpr: {} -> Terminal: {}", prefix(), ctx.getText(), terminals);
        level++;
    }

    @Override
    public void exitExpr(final DataPrepperScriptParser.ExprContext ctx) {
        final String terminals = getTerminalString(ctx);
        level--;
        LOG.info("{}exitExpr: {} -> Terminal: {}", prefix(), ctx.getText(), terminals);
    }

    @Override
    public void enterGroup(final DataPrepperScriptParser.GroupContext ctx) {
        LOG.info("{}enterGroup: {}", prefix(), ctx.getText());
        level++;

    }

    @Override
    public void exitGroup(final DataPrepperScriptParser.GroupContext ctx) {
        level--;
        LOG.info("{}exitGroup: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterListItems(final DataPrepperScriptParser.ListItemsContext ctx) {
        LOG.info("{}enterListItems: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitListItems(final DataPrepperScriptParser.ListItemsContext ctx) {
        level--;
        LOG.info("{}exitListItems: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterList(final DataPrepperScriptParser.ListContext ctx) {
        LOG.info("{}enterList: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitList(final DataPrepperScriptParser.ListContext ctx) {
        level--;
        LOG.info("{}exitList: {}", prefix(), ctx.getText());
    }

    @Override
    public void enterTerm(final DataPrepperScriptParser.TermContext ctx) {
        LOG.info("{}enterTerm: {}", prefix(), ctx.getText());
        level++;
    }

    @Override
    public void exitTerm(final DataPrepperScriptParser.TermContext ctx) {
        level--;
        LOG.info("{}exitTerm: {}", prefix(), ctx.getText());
    }

    @Override
    public void visitTerminal(final TerminalNode node) {
        LOG.info("{}visitTerminal: {}", prefix(), node.getText());
    }

    @Override
    public void visitErrorNode(final ErrorNode node) {
        LOG.info("visitErrorNode: {}", node.getText());
    }

    @Override
    public void enterEveryRule(final ParserRuleContext ctx) {
//        LOG.info("{}enterEveryRule: {}", prefix(), ctx.getText());
//        level++;
    }

    @Override
    public void exitEveryRule(final ParserRuleContext ctx) {
//        level--;
//        LOG.info("{}exitEveryRule: {}", prefix(), ctx.getText());
    }
}
