/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression.util;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.expression.util.LiteralMatcher.isUnaryTree;
import static org.opensearch.dataprepper.expression.util.ParenthesesExpressionMatcher.isParenthesesExpression;

class ParenthesesExpressionMatcherTest {

    @Test
    void testGivenValidParseTreeMatchIsTrue() {
        final DiagnosingMatcher<ParseTree> isParenthesesExpression = isParenthesesExpression(isUnaryTree());
        final ParseTree relationalOperatorExpressionContext =
                mock(DataPrepperExpressionParser.RelationalOperatorExpressionContext.class, "RelationalOperatorExpression");
        final ParseTree setOperatorExpressionContext =
                mock(DataPrepperExpressionParser.SetOperatorExpressionContext.class, "SetOperatorExpression");
        final ParseTree unaryOperatorExpressionContext =
                mock(DataPrepperExpressionParser.UnaryOperatorExpressionContext.class, "UnaryOperatorExpression");
        final ParseTree parenthesesExpressionContext =
                mock(DataPrepperExpressionParser.ParenthesesExpressionContext.class, "ParenthesesExpression");
        final ParseTree terminal = mock(TerminalNode.class, "TerminalNode");
        final ParseTree primary = mock(DataPrepperExpressionParser.PrimaryContext.class, "PrimaryContext");
        final ParseTree literal = mock(DataPrepperExpressionParser.LiteralContext.class, "LiteralContext");

        doReturn(1).when(relationalOperatorExpressionContext).getChildCount();
        doReturn(1).when(setOperatorExpressionContext).getChildCount();
        doReturn(1).when(unaryOperatorExpressionContext).getChildCount();
        doReturn(3).when(parenthesesExpressionContext).getChildCount();
        doReturn(1).when(primary).getChildCount();
        doReturn(1).when(literal).getChildCount();

        doReturn(setOperatorExpressionContext).when(relationalOperatorExpressionContext).getChild(eq(0));
        doReturn(unaryOperatorExpressionContext).when(setOperatorExpressionContext).getChild(eq(0));
        doReturn(parenthesesExpressionContext).when(unaryOperatorExpressionContext).getChild(eq(0));
        doReturn(terminal).when(parenthesesExpressionContext).getChild(eq(0));
        doReturn(primary).when(parenthesesExpressionContext).getChild(eq(1));
        doReturn(terminal).when(parenthesesExpressionContext).getChild(eq(2));
        doReturn(literal).when(primary).getChild(eq(0));
        doReturn(terminal).when(literal).getChild(eq(0));

        assertTrue(isParenthesesExpression.matches(relationalOperatorExpressionContext));
    }

    @Test
    void testGivenInvalidParseTreeRootMatcherFails() {
        final DiagnosingMatcher<ParseTree> isParenthesesExpression = isParenthesesExpression(isUnaryTree());

        final ParseTree expressionContext =
                mock(DataPrepperExpressionParser.ExpressionContext.class, "ExpressionContext");

        doReturn("").when(expressionContext).getText();

        assertFalse(isParenthesesExpression.matches(expressionContext));
    }

    @Test
    void testGivenInvalidParseTreeMatchFails() {
        final DiagnosingMatcher<ParseTree> isParenthesesExpression = isParenthesesExpression(isUnaryTree());
        final ParseTree relationalOperatorExpressionContext =
                mock(DataPrepperExpressionParser.RelationalOperatorExpressionContext.class, "RelationalOperatorExpression");
        final ParseTree setOperatorExpressionContext =
                mock(DataPrepperExpressionParser.SetOperatorExpressionContext.class, "SetOperatorExpression");
        final ParseTree unaryOperatorExpressionContext =
                mock(DataPrepperExpressionParser.UnaryOperatorExpressionContext.class, "UnaryOperatorExpression");
        final ParseTree parenthesesExpressionContext =
                mock(DataPrepperExpressionParser.ParenthesesExpressionContext.class, "ParenthesesExpression");

        doReturn(1).when(relationalOperatorExpressionContext).getChildCount();
        doReturn(1).when(setOperatorExpressionContext).getChildCount();
        doReturn(1).when(unaryOperatorExpressionContext).getChildCount();
        doReturn(2).when(parenthesesExpressionContext).getChildCount();

        doReturn(setOperatorExpressionContext).when(relationalOperatorExpressionContext).getChild(eq(0));
        doReturn(unaryOperatorExpressionContext).when(setOperatorExpressionContext).getChild(eq(0));
        doReturn(parenthesesExpressionContext).when(unaryOperatorExpressionContext).getChild(eq(0));


        doReturn("").when(parenthesesExpressionContext).getText();

        assertFalse(isParenthesesExpression.matches(relationalOperatorExpressionContext));
    }
}
