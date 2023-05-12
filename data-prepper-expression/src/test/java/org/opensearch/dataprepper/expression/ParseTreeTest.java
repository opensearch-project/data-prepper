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

import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isExpression;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isOperator;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.JsonPointerMatcher.isJsonPointerUnaryTree;
import static org.opensearch.dataprepper.expression.util.FunctionMatcher.isFunctionUnaryTree;
import static org.opensearch.dataprepper.expression.util.LiteralMatcher.isUnaryTree;

class ParseTreeTest extends GrammarTest {

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
    void testGivenOperandIsNotStringWhenEvaluateThenErrorPresent() {
        parseExpression("(true) =~ \"Hello?\"");

        assertThat(errorListener.isErrorFound(), is(true));
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
    void testFunction() {
        final ParserRuleContext expression = parseExpression("length(\"abcd\") == 4");

        assertThat(expression, isExpression(isParseTree(
                        CONDITIONAL_EXPRESSION,
                        EQUALITY_OPERATOR_EXPRESSION
                ).withChildrenMatching(
                        isFunctionUnaryTree(),
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
    void testValidJsonPointerCharacterSet() {
        final ParserRuleContext expression = parseExpression(
                "/ABCDEFGHIJKLMNOPQRSTUVWXYZ/abcdefghijklmnopqrstuvwxyz/0123456789/_");
        assertThat(expression, isExpression(isJsonPointerUnaryTree()));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testSubtractOperator() {
        final ParserRuleContext expression = parseExpression("/status_code == -200");

        final DiagnosingMatcher<ParseTree> lhs = isJsonPointerUnaryTree();
        final Function<DiagnosingMatcher<ParseTree>, DiagnosingMatcher<ParseTree>> isNegative =
                rhs -> hasContext(UNARY_OPERATOR_EXPRESSION, isOperator(UNARY_OPERATOR), rhs);
        final DiagnosingMatcher<ParseTree> doubleNegative200 = isNegative.apply(isUnaryTree());

        final DiagnosingMatcher<ParseTree> rhs = isParseTree(
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                doubleNegative200
        );

        final DiagnosingMatcher<ParseTree> equalsExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                lhs,
                isOperator(EQUALITY_OPERATOR),
                rhs
        );


        assertThat(expression, isExpression(equalsExpression));
        assertThat(errorListener.isErrorFound(), is(false));
    }
}
