/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isOperator;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isRegexString;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isPrimaryLiteral;
import static org.opensearch.dataprepper.expression.util.ParseRuleContextExceptionMatcher.isNotValid;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

public class ParserTest {
    //region ParseTree child classes
    private static final Class<? extends ParseTree> EXPRESSION = DataPrepperExpressionParser.ExpressionContext.class;
    private static final Class<? extends ParseTree> CONDITIONAL_EXPRESSION = DataPrepperExpressionParser.ConditionalExpressionContext.class;
    private static final Class<? extends ParseTree> CONDITIONAL_OPERATOR = DataPrepperExpressionParser.ConditionalOperatorContext.class;
    private static final Class<? extends ParseTree> EQUALITY_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.EqualityOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> EQUALITY_OPERATOR = DataPrepperExpressionParser.EqualityOperatorContext.class;
    private static final Class<? extends ParseTree> REGEX_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.RegexOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> REGEX_EQUALITY_OPERATOR =
            DataPrepperExpressionParser.RegexEqualityOperatorContext.class;
    private static final Class<? extends ParseTree> RELATIONAL_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.RelationalOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> RELATIONAL_OPERATOR = DataPrepperExpressionParser.RelationalOperatorContext.class;
    private static final Class<? extends ParseTree> SET_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.SetOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> SET_OPERATOR = DataPrepperExpressionParser.SetOperatorContext.class;
    private static final Class<? extends ParseTree> UNARY_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.UnaryOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> PARENTHESIS_EXPRESSION = DataPrepperExpressionParser.ParenthesisExpressionContext.class;
    private static final Class<? extends ParseTree> REGEX_PATTERN = DataPrepperExpressionParser.RegexPatternContext.class;
    private static final Class<? extends ParseTree> SET_INITIALIZER = DataPrepperExpressionParser.SetInitializerContext.class;
    private static final Class<? extends ParseTree> UNARY_NOT_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.UnaryNotOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> UNARY_OPERATOR = DataPrepperExpressionParser.UnaryOperatorContext.class;
    private static final Class<? extends ParseTree> PRIMARY = DataPrepperExpressionParser.PrimaryContext.class;
    private static final Class<? extends ParseTree> JSON_POINTER = DataPrepperExpressionParser.JsonPointerContext.class;
    private static final Class<? extends ParseTree> LITERAL = DataPrepperExpressionParser.LiteralContext.class;
    //endregion

    private DataPrepperExpressionParser parser;

    private void setExpression(final String expression) {
        final CodePointCharStream stream = CharStreams.fromString(expression);
        final DataPrepperExpressionLexer lexer = new DataPrepperExpressionLexer(stream);
        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        parser = new DataPrepperExpressionParser(tokenStream);
    }

    @Test
    void testEqualityExpression() {
        setExpression("5==5");

        final ParserRuleContext expression = parser.expression();

        final DiagnosingMatcher<ParseTree> equals = isParseTree(EQUALITY_OPERATOR).containingTerminalNode();
        final DiagnosingMatcher<ParseTree> endsWithInteger = isParseTree(
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION,
                UNARY_OPERATOR_EXPRESSION,
                PRIMARY,
                LITERAL
        ).containingTerminalNode();
        final DiagnosingMatcher<ParseTree> leftHandSide = hasContext(EQUALITY_OPERATOR_EXPRESSION, endsWithInteger);
        final DiagnosingMatcher<ParseTree> equalityExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(leftHandSide, equals, endsWithInteger);

        assertThat(expression, isParseTree(EXPRESSION).withChildrenMatching(equalityExpression, isTerminalNode())
        );
    }

    @Test
    void testGivenOperandIsNotStringWhenEvaluateThenErrorPresent() {
        setExpression("(true) =~ \"Hello?\"");

        final ParserRuleContext expression = parser.expression();

        assertThat(expression, isNotValid());
    }

    @Test
    void testParenthesisExpression() {
        setExpression("(\"string\" =~ \"Hello?\") or 5 in {1, \"Hello\", 3.14, true}");

        final ParserRuleContext expression = parser.expression();

        //region Build Parse Tree Assertion
        final DiagnosingMatcher<ParseTree> regexExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION
        ).withChildrenMatching(isRegexString(), isOperator(REGEX_EQUALITY_OPERATOR), isRegexString());
        final DiagnosingMatcher<ParseTree> parenthesisExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION,
                UNARY_OPERATOR_EXPRESSION,
                PARENTHESIS_EXPRESSION
        ).withChildrenMatching(
                isTerminalNode(),
                regexExpression,
                isTerminalNode()
        );
        final DiagnosingMatcher<ParseTree> setOperatorLhs = isParseTree(SET_OPERATOR_EXPRESSION, UNARY_OPERATOR_EXPRESSION)
                .withChildrenMatching(isPrimaryLiteral());
        final DiagnosingMatcher<ParseTree> set = hasContext(
                SET_INITIALIZER,
                isTerminalNode(),
                isPrimaryLiteral(),
                isTerminalNode(),
                isPrimaryLiteral(),
                isTerminalNode(),
                isPrimaryLiteral(),
                isTerminalNode(),
                isPrimaryLiteral(),
                isTerminalNode()
        );
        final DiagnosingMatcher<ParseTree> setOperatorExpression = isParseTree(
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                setOperatorLhs,
                isOperator(SET_OPERATOR),
                set
        );
        final DiagnosingMatcher<ParseTree> conditionalExpression = hasContext(
                CONDITIONAL_EXPRESSION,
                parenthesisExpression,
                isOperator(CONDITIONAL_OPERATOR),
                setOperatorExpression
        );
        //endregion

        assertThat(expression, hasContext(EXPRESSION, conditionalExpression, isTerminalNode()));

    }
}
