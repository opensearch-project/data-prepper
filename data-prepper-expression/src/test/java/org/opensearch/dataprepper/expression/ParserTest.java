/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.ErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isExpression;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isOperator;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isRegexString;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isUnaryTreeSet;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isPrimaryLiteral;
import static org.opensearch.dataprepper.expression.util.LiteralMatcher.isUnaryTree;
import static org.opensearch.dataprepper.expression.util.ParenthesesExpressionMatcher.isParenthesesExpression;
import static org.opensearch.dataprepper.expression.util.ParseRuleContextExceptionMatcher.isNotValid;
import static org.opensearch.dataprepper.expression.util.TerminalNodeMatcher.isTerminalNode;

public class ParserTest {
    private static final Logger LOG = LoggerFactory.getLogger(ParserTest.class);
    private static final CharStream EMPTY_STREAM = CharStreams.fromString("");

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
    private static final Class<? extends ParseTree> PARENTHESES_EXPRESSION = DataPrepperExpressionParser.ParenthesesExpressionContext.class;
    private static final Class<? extends ParseTree> REGEX_PATTERN = DataPrepperExpressionParser.RegexPatternContext.class;
    private static final Class<? extends ParseTree> SET_INITIALIZER = DataPrepperExpressionParser.SetInitializerContext.class;
    private static final Class<? extends ParseTree> UNARY_NOT_OPERATOR_EXPRESSION =
            DataPrepperExpressionParser.UnaryNotOperatorExpressionContext.class;
    private static final Class<? extends ParseTree> UNARY_OPERATOR = DataPrepperExpressionParser.UnaryOperatorContext.class;
    private static final Class<? extends ParseTree> PRIMARY = DataPrepperExpressionParser.PrimaryContext.class;
    private static final Class<? extends ParseTree> JSON_POINTER = DataPrepperExpressionParser.JsonPointerContext.class;
    private static final Class<? extends ParseTree> LITERAL = DataPrepperExpressionParser.LiteralContext.class;
    //endregion

    private ErrorListener errorListener;

    private ParserRuleContext parseExpression(final String expression) {
        errorListener = new ErrorListener();

        final CodePointCharStream stream = CharStreams.fromString(expression);
        final DataPrepperExpressionLexer lexer = new DataPrepperExpressionLexer(stream);
        lexer.addErrorListener(errorListener);

        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        final DataPrepperExpressionParser parser = new DataPrepperExpressionParser(tokenStream);
        parser.addErrorListener(errorListener);
        parser.setTrace(true);

        final ParserRuleContext context = parser.expression();

        final ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(errorListener, context);

        return context;
    }

    private Executable assertThatHasParseError(final String statement) {
        return () -> {
            parseExpression(statement);
            assertThat(
                    "\"" + statement + "\" should have parsing errors",
                    errorListener.isErrorFound(),
                    is(true)
            );
        };
    }

    @Test
    void testEqualityExpression() {
        final ParserRuleContext expression = parseExpression("5==5");

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

        assertThat(expression, isParseTree(EXPRESSION).withChildrenMatching(equalityExpression, isTerminalNode()));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testGivenOperandIsNotStringWhenEvaluateThenErrorPresent() {
        final ParserRuleContext expression = parseExpression("(true) =~ \"Hello?\"");

        assertThat(expression, isNotValid());
        assertThat(errorListener.isErrorFound(), is(true));
    }

    @Test
    void testParenthesesExpression() {
        final ParserRuleContext expression = parseExpression("(\"string\" =~ \"Hello?\") or 5 in {1, \"Hello\", 3.14, true}");

        //region Build Parse Tree Assertion
        final DiagnosingMatcher<ParseTree> regexExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION
        ).withChildrenMatching(isRegexString(), isOperator(REGEX_EQUALITY_OPERATOR), isRegexString());
        final DiagnosingMatcher<ParseTree> parenthesesExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION,
                UNARY_OPERATOR_EXPRESSION,
                PARENTHESES_EXPRESSION
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
                parenthesesExpression,
                isOperator(CONDITIONAL_OPERATOR),
                setOperatorExpression
        );
        //endregion

        assertThat(expression, hasContext(EXPRESSION, conditionalExpression, isTerminalNode()));
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
    void test() {
        final ParserRuleContext expression = parseExpression("2 in {\"1\", 2, 3}");

        final DiagnosingMatcher<ParseTree> setOperatorExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION,
                REGEX_OPERATOR_EXPRESSION,
                RELATIONAL_OPERATOR_EXPRESSION,
                SET_OPERATOR_EXPRESSION
        ).withChildrenMatching(
                isUnaryTree(),
                isOperator(SET_OPERATOR),
                isUnaryTreeSet(3)
        );

        assertThat(expression, isExpression(setOperatorExpression));
        assertThat(errorListener.isErrorFound(), is(false));
    }

    @Test
    void testInvalidSetItem() {
        assertAll(
                assertThatHasParseError("{true or false}"),
                assertThatHasParseError("{(5)}"),
                assertThatHasParseError("{5 == 5}"),
                assertThatHasParseError("{/node =~ \"[A-Z]*\"}")
        );
    }

    @Test
    void testMultipleParentheses() {
        final ParserRuleContext expression = parseExpression("(1==4)or((2)!=(3==3))");

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

//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("/a/b/c == true");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"Hello World\" == 42");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"Hello \\\"World\\\"\" == 42");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"/a b/\\\"c~d\\\"/\\/\"");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("1 < 2");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("1 < 2 or 3 <= 4 or 5 > 6 or 7 >= 8");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("1 in 1");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("3 > 1 or (/status_code == 500)");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("3>1or(/status_code==500)");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("/ABCDEFGHIJKLMNOPQRSTUVWXYZ/ambcdefghijklmnopqrstuvwxyz/0123456789/_");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"Hello, this \\\"is\\\" a 'complex' ~ string with numbers! 0123\"");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"foo\"=~\"[A-Z]*\"");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"foo\"!~\"[A-Z]*\"");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("3.14159");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"foo\"=~3.14");
//    }

    @Test
    void testInvalidFloatParsingRules() {
        assertAll(
                assertThatHasParseError("0."),
                assertThatHasParseError("00."),
                assertThatHasParseError(".00"),
                assertThatHasParseError(".10"),
                assertThatHasParseError("1.10")
        );
    }

    private Executable assertThatIsValidLiteral(final String statement) {
        return () -> {
            final ParserRuleContext literal = parseExpression(statement);
            assertThat(
                    "\"" + statement + "\" should be valid expression with unary tree child and <EOF> symbol",
                    literal,
                    isExpression(isUnaryTree())
            );
            assertThat(
                    "Statement \"" + statement + "\" should have no errors",
                    errorListener.isErrorFound(),
                    is(false)
            );
        };
    }

    @Test
    void testValidFloatParsingRules() {
        assertAll(
                assertThatIsValidLiteral("1.0"),
                assertThatIsValidLiteral(".0"),
                assertThatIsValidLiteral(".1")
        );
    }

//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("\"Hello \\\"world\\\", 'this' is a \\\\ string.\"");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("5==");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("==");
//    }
//    @Test
//    void test() {
//        final ParserRuleContext expression = parseExpression("not (5 not in [1]) or not \"in\"");
//    }
}
