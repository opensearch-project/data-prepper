/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.MockTokenStreamHelper;
import org.opensearch.dataprepper.expression.util.StubCommonTokenStream;
import org.opensearch.dataprepper.expression.util.TokenStreamSpy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isOperator;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isRegexString;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isPrimaryLiteral;
import static org.opensearch.dataprepper.expression.util.ParseRuleContextExceptionMatcher.isNotValid;
import static org.opensearch.dataprepper.expression.util.StubCommonTokenStream.stubTokenStream;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

@ExtendWith(MockitoExtension.class)
public class ParserTest {
    private static final Logger LOG = LoggerFactory.getLogger(ParserTest.class);

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

    @Mock
    private CommonTokenStream tokenStream;
    @InjectMocks
    private DataPrepperExpressionParser parser;
    MockTokenStreamHelper ref = null;

    private void withTokenStream(final Integer ... types) {
        final MockTokenStreamHelper helper = new MockTokenStreamHelper(types);
        ref = helper;

        doAnswer(helper::seek).when(tokenStream).seek(anyInt());
        doAnswer(helper::LT).when(tokenStream).LT(anyInt());
        doAnswer(helper::LA).when(tokenStream).LA(anyInt());

        final Lexer lexer = new DataPrepperExpressionLexer(CharStreams.fromString("5==5"));
        final CommonTokenStream tokenStream = new TokenStreamSpy(lexer);

        parser.setInputStream(tokenStream);
    }

    @Test
    void testEqualityExpression() {
//        withTokenStream(
//                DataPrepperExpressionParser.Integer,
//                DataPrepperExpressionParser.EQUAL,
//                DataPrepperExpressionParser.Integer,
//                DataPrepperExpressionParser.EOF
//        );
        final StubCommonTokenStream input = stubTokenStream();
        parser.setInputStream(input);

        input.addToken(DataPrepperExpressionParser.Integer);
        input.addToken(DataPrepperExpressionParser.EQUAL);
        input.addToken(DataPrepperExpressionParser.Integer);
        input.addToken(DataPrepperExpressionParser.EOF);

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
        withTokenStream(
                DataPrepperExpressionParser.LPAREN,
                DataPrepperExpressionParser.Boolean,
                DataPrepperExpressionParser.RPAREN,
                DataPrepperExpressionParser.MATCH_REGEX_PATTERN,
                DataPrepperExpressionParser.String,
                DataPrepperExpressionParser.EOF
        );

        final ParserRuleContext expression = parser.expression();

        assertThat(expression, isNotValid());
    }

    @Test
    void testParenthesisExpression() {
        withTokenStream(
                DataPrepperExpressionParser.LPAREN,
                DataPrepperExpressionParser.String,
                DataPrepperExpressionParser.MATCH_REGEX_PATTERN,
                DataPrepperExpressionParser.String,
                DataPrepperExpressionParser.RPAREN,
                DataPrepperExpressionParser.OR,
                DataPrepperExpressionParser.Integer,
                DataPrepperExpressionParser.IN_SET,
                DataPrepperExpressionParser.LBRACE,
                DataPrepperExpressionParser.Integer,
                DataPrepperExpressionParser.SET_SEPARATOR,
                DataPrepperExpressionParser.String,
                DataPrepperExpressionParser.SET_SEPARATOR,
                DataPrepperExpressionParser.Float,
                DataPrepperExpressionParser.SET_SEPARATOR,
                DataPrepperExpressionParser.Boolean,
                DataPrepperExpressionParser.RBRACE,
                DataPrepperExpressionParser.EOF
        );
        final ParserRuleContext expression = parser.expression();

        //region Build Parse Tree Assertion
        final DiagnosingMatcher<ParseTree> regexExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION
        ).withChildrenMatching(isRegexString(), isOperator(REGEX_OPERATOR_EXPRESSION), isRegexString());
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

//    @Test
//    void foo() {
//        final Lexer lexer = new DataPrepperExpressionLexer(CharStreams.fromString("(\"hi\" =~ \"hello\") or 5 in {1, \"foo\", 3.14, false}"));
//        final CommonTokenStream tokenStream = new TokenStreamSpy(lexer);
//        final DataPrepperExpressionParser expressionParser = new DataPrepperExpressionParser(tokenStream);
//        final ParserRuleContext expression = expressionParser.expression();
//        assertThat(expression, isValid());
//    }
}
