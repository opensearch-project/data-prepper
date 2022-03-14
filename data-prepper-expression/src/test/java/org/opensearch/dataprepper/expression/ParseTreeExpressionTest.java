/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.util.GrammarTest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isExpression;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isOperator;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.JsonPointerMatcher.isJsonPointerUnaryTree;
import static org.opensearch.dataprepper.expression.util.LiteralMatcher.isUnaryTree;
import static org.opensearch.dataprepper.expression.util.ParenthesesExpressionMatcher.isParenthesesExpression;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

class ParseTreeExpressionTest extends GrammarTest {

    @Test
    void testEqualityExpression() {
        final ParserRuleContext expression = parseExpression("5==5");

        final DiagnosingMatcher<ParseTree> equalityExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(isUnaryTree(), isOperator(EQUALITY_OPERATOR), isUnaryTree());

        assertThat(expression, isExpression(equalityExpression));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testParenthesesExpression() {
        final ParserRuleContext expression = parseExpression("(/json/pointer == \"string value\") or (/status_code < 300)");

        final DiagnosingMatcher<ParseTree> lhs = isParenthesesExpression(isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isJsonPointerUnaryTree(),
                isOperator(EQUALITY_OPERATOR),
                isUnaryTree()
        ));

        final DiagnosingMatcher<ParseTree> rhs = isParenthesesExpression(isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isJsonPointerUnaryTree(),
                isOperator(RELATIONAL_OPERATOR),
                isUnaryTree()
        ));

        assertThat(expression, isExpression(hasContext(CONDITIONAL_EXPRESSION, lhs, isOperator(CONDITIONAL_OPERATOR), rhs)));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testEquality() {
        final ParserRuleContext expression = parseExpression("true==false");

        final DiagnosingMatcher<ParseTree> equalityExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(isUnaryTree(), isOperator(EQUALITY_OPERATOR), isUnaryTree());

        assertThat(expression, isExpression(equalityExpression));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testConditionalExpression() {
        final ParserRuleContext expression = parseExpression("false and true");

        assertThat(expression, isExpression(hasContext(
                CONDITIONAL_EXPRESSION,
                isUnaryTree(),
                isOperator(CONDITIONAL_OPERATOR),
                isUnaryTree()
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testMultipleConditionExpressions() {
        final ParserRuleContext expression = parseExpression("false and true or true");
        assertThat(expression, isExpression(hasContext(
                CONDITIONAL_EXPRESSION,
                hasContext(
                        CONDITIONAL_EXPRESSION,
                        isUnaryTree(),
                        isOperator(CONDITIONAL_OPERATOR),
                        isUnaryTree()
                ),
                isOperator(CONDITIONAL_OPERATOR),
                isUnaryTree()
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testParenthesesAndConditionExpression() {
        final ParserRuleContext expression = parseExpression("false and (false or true)");


        final DiagnosingMatcher<ParseTree> conditionalExpression = hasContext(
                CONDITIONAL_EXPRESSION,
                isUnaryTree(),
                isOperator(CONDITIONAL_OPERATOR),
                isUnaryTree()
        );

        final DiagnosingMatcher<ParseTree> parenthesesExpression = isParseTree(
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION,
                UNARY_OPERATOR_EXPRESSION,
                PARENTHESES_EXPRESSION
        ).withChildrenMatching(
                isTerminalNode(),
                conditionalExpression,
                isTerminalNode()
        );

        assertThat(expression, isExpression(hasContext(
                CONDITIONAL_EXPRESSION,
                isUnaryTree(),
                isOperator(CONDITIONAL_OPERATOR),
                parenthesesExpression
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testMultipleParentheses() {
        final ParserRuleContext expression = parseExpression("(1==4) or ((2)!=(3==3))");

        final DiagnosingMatcher<ParseTree> threeEqualThree = isParenthesesExpression(
                isParseTree(CONDITIONAL_EXPRESSION, EQUALITY_OPERATOR_EXPRESSION)
                        .withChildrenMatching(isUnaryTree(), isOperator(EQUALITY_OPERATOR), isUnaryTree())
        );

        final DiagnosingMatcher<ParseTree> lhsEqualityExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(isUnaryTree(), isOperator(EQUALITY_OPERATOR), isUnaryTree());

        final DiagnosingMatcher<ParseTree> lhsExpression = hasContext(
                CONDITIONAL_EXPRESSION,
                isParenthesesExpression(lhsEqualityExpression)
        );

        final DiagnosingMatcher<ParseTree> rhsNotEqualExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isParenthesesExpression(isUnaryTree()),
                isOperator(EQUALITY_OPERATOR),
                threeEqualThree
        );

        assertThat(expression, isExpression(hasContext(
                CONDITIONAL_EXPRESSION,
                lhsExpression,
                isOperator(CONDITIONAL_OPERATOR),
                isParenthesesExpression(rhsNotEqualExpression)
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testJsonPointer() {
        final ParserRuleContext expression = parseExpression("/a/b/c == true");

        assertThat(expression, isExpression(isParseTree(
                        CONDITIONAL_EXPRESSION,
                        EQUALITY_OPERATOR_EXPRESSION
                ).withChildrenMatching(
                        isJsonPointerUnaryTree(),
                        isOperator(EQUALITY_OPERATOR),
                        isUnaryTree()
                )
        ));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testSimpleString() {
        final ParserRuleContext expression = parseExpression("\"Hello World\" == 42");

        assertThat(expression, isExpression(isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isUnaryTree(),
                isOperator(EQUALITY_OPERATOR),
                isUnaryTree()
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testStringEscapeCharacters() {
        final ParserRuleContext expression = parseExpression("\"Hello \\\"World\\\"\" == 42");

        assertThat(expression, isExpression(isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isUnaryTree(),
                isOperator(EQUALITY_OPERATOR),
                isUnaryTree()
        )));
        assertThat(errorListener.isErrorFound(), is(false));

    }

    @Test
    void testJsonPointerEscapeCharacters() {
        final ParserRuleContext expression = parseExpression("\"/a b/\\\"c~d\\\"/\\/\"");
        assertThat(expression, isExpression(isJsonPointerUnaryTree()));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testRelationalExpression() {
        final ParserRuleContext expression = parseExpression("1 < 2");

        assertThat(expression, isExpression(isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isUnaryTree(),
                isOperator(RELATIONAL_OPERATOR),
                isUnaryTree()
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testNestedConditionalExpression() {
        final ParserRuleContext expression = parseExpression("1 < 2 or 3 <= 4 or 5 > 6 or 7 >= 8");

        final DiagnosingMatcher<ParseTree> relationalOperatorExpression = isParseTree(
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isUnaryTree(),
                isOperator(RELATIONAL_OPERATOR),
                isUnaryTree()
        );
        final DiagnosingMatcher<ParseTree> firstConditional = hasContext(
                CONDITIONAL_EXPRESSION,
                hasContext(CONDITIONAL_EXPRESSION, relationalOperatorExpression),
                isOperator(CONDITIONAL_OPERATOR),
                relationalOperatorExpression
        );
        final DiagnosingMatcher<ParseTree> secondConditional = hasContext(
                CONDITIONAL_EXPRESSION,
                firstConditional,
                isOperator(CONDITIONAL_OPERATOR),
                relationalOperatorExpression
        );
        final DiagnosingMatcher<ParseTree> thirdConditional = hasContext(
                CONDITIONAL_EXPRESSION,
                secondConditional,
                isOperator(CONDITIONAL_OPERATOR),
                relationalOperatorExpression
        );

        assertThat(expression, isExpression(thirdConditional));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testNestedJsonPointer() {
        final ParserRuleContext expression = parseExpression("3 > 1 or (/status_code == 500)");

        final DiagnosingMatcher<ParseTree> relationalOperatorExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isUnaryTree(),
                isOperator(RELATIONAL_OPERATOR),
                isUnaryTree()
        );
        final DiagnosingMatcher<ParseTree> parenthesesExpression = isParenthesesExpression(isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isJsonPointerUnaryTree(),
                isOperator(EQUALITY_OPERATOR),
                isUnaryTree()
        ));

        assertThat(expression, isExpression(hasContext(
                CONDITIONAL_EXPRESSION,
                relationalOperatorExpression,
                isOperator(CONDITIONAL_OPERATOR),
                parenthesesExpression
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testValidJsonPointerCharacterSet() {
        final ParserRuleContext expression = parseExpression(
                "/ABCDEFGHIJKLMNOPQRSTUVWXYZ/ambcdefghijklmnopqrstuvwxyz/0123456789/_");
        assertThat(expression, isExpression(isJsonPointerUnaryTree()));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testNotOperationOrder() {
        final ParserRuleContext expression = parseExpression("not false or true");

        final DiagnosingMatcher<ParseTree> lhs = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION,
                UNARY_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isOperator(UNARY_OPERATOR),
                isUnaryTree()
        );
        final DiagnosingMatcher<ParseTree> rhs = isUnaryTree();

        assertThat(expression, isExpression(hasContext(
                CONDITIONAL_EXPRESSION,
                lhs,
                isOperator(CONDITIONAL_OPERATOR),
                rhs
        )));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testNotPriorityOperationOrder() {
        final ParserRuleContext expression = parseExpression("not (false or true)");

        final DiagnosingMatcher<ParseTree> parenthesesExpression = isParenthesesExpression(hasContext(
                CONDITIONAL_EXPRESSION,
                isUnaryTree(),
                isOperator(CONDITIONAL_OPERATOR),
                isUnaryTree()
        ));

        final DiagnosingMatcher<ParseTree> not = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION,
                UNARY_OPERATOR_EXPRESSION
        ).withChildrenMatching(isOperator(UNARY_OPERATOR), parenthesesExpression);

        assertThat(expression, isExpression(not));
        assertThat(errorListener.isErrorFound(), is(false));
    }
}
