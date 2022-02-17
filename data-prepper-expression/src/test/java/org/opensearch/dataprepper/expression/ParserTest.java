/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hamcrest.DiagnosingMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionBaseListener;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.hasContext;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isExpression;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isOperator;
import static org.opensearch.dataprepper.expression.util.ContextMatcher.isRegexString;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isParseTree;
import static org.opensearch.dataprepper.expression.util.ContextMatcherFactory.isPrimaryLiteral;
import static org.opensearch.dataprepper.expression.util.LiteralMatcher.isUnaryTree;
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

    final ANTLRErrorListener errorListener = new ANTLRErrorListener() {
        @Override
        public void syntaxError(
                final Recognizer<?, ?> recognizer,
                final Object offendingSymbol,
                final int line,
                final int charPositionInLine,
                final String msg,
                final RecognitionException e
        ) {
            LOG.error("Syntax Error! syntaxError");
            foundErrorNode = true;
        }

        @Override
        public void reportAmbiguity(
                final Parser recognizer,
                final DFA dfa,
                final int startIndex,
                final int stopIndex,
                final boolean exact,
                final BitSet ambigAlts,
                final ATNConfigSet configs
        ) {
            LOG.error("Syntax Error! reportAmbiguity");
            foundErrorNode = true;
        }

        @Override
        public void reportAttemptingFullContext(
                final Parser recognizer,
                final DFA dfa,
                final int startIndex,
                final int stopIndex,
                final BitSet conflictingAlts,
                final ATNConfigSet configs
        ) {
            LOG.error("Syntax Error! reportAttemptingFullContext");
            foundErrorNode = true;
        }

        @Override
        public void reportContextSensitivity(
                final Parser recognizer,
                final DFA dfa,
                final int startIndex,
                final int stopIndex,
                final int prediction,
                final ATNConfigSet configs
        ) {
            LOG.error("Syntax Error! reportContextSensitivity");
            foundErrorNode = true;
        }
    };

    final ParseTreeListener listener = new DataPrepperExpressionBaseListener() {
        @Override
        public void enterEveryRule(final ParserRuleContext ctx) {
            LOG.trace("Enter Rule {}", ctx.getText());
            if (ctx.exception != null) {
                foundErrorNode = true;
            }
        }

        @Override
        public void visitErrorNode(final ErrorNode node) {
            foundErrorNode = true;
        }
    };

    private DataPrepperExpressionLexer lexer;
    private DataPrepperExpressionParser parser;
    private final ParseTreeWalker walker = new ParseTreeWalker();
    private boolean foundErrorNode = false;

    @BeforeEach
    public void beforeEach() {
        foundErrorNode = false;

        lexer = new DataPrepperExpressionLexer(EMPTY_STREAM);
        lexer.addErrorListener(errorListener);

        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        parser = new DataPrepperExpressionParser(tokenStream);
        parser.addErrorListener(errorListener);
        parser.setTrace(true);
    }

    private ParserRuleContext parseExpression(final String expression) {
        foundErrorNode = false;

        final CodePointCharStream stream = CharStreams.fromString(expression);
        lexer.setInputStream(stream);

        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        parser.setTokenStream(tokenStream);

        final ParserRuleContext context = parser.expression();
        walker.walk(listener, context);

        return context;
    }

    @Test
    void testEqualityExpression() {
        parseExpression("5==5");

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
        parseExpression("(true) =~ \"Hello?\"");

        final ParserRuleContext expression = parser.expression();

        assertThat(expression, isNotValid());
    }

    @Test
    void testParenthesisExpression() {
        parseExpression("(\"string\" =~ \"Hello?\") or 5 in {1, \"Hello\", 3.14, true}");

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

    @Test
    void testEquality() {
        parseExpression("true==false");
        final ParserRuleContext expression = parser.expression();

        final DiagnosingMatcher<ParseTree> equalityExpression = isParseTree(
                CONDITIONAL_EXPRESSION,
                EQUALITY_OPERATOR_EXPRESSION
        ).withChildrenMatching(isUnaryTree(), isOperator(EQUALITY_OPERATOR), isUnaryTree());

        assertThat(expression, isExpression(equalityExpression));
    }
//    @Test
//    void test() {
//        setExpression("false and true");
//    }
//    @Test
//    void test() {
//        setExpression("false and true or true");
//    }
//    @Test
//    void test() {
//        setExpression("false and (false or true)");
//    }
//    @Test
//    void test() {
//        setExpression("2 in {\"1\", 2, 3}");
//    }
//    @Test
//    void test() {
//        setExpression("true not in {false, true or false}");
//    }
//    @Test
//    void test() {
//        setExpression("(1==4)or((2)!=(3==3))");
//    }
//    @Test
//    void test() {
//        setExpression("/a/b/c == true");
//    }
//    @Test
//    void test() {
//        setExpression("\"Hello World\" == 42");
//    }
//    @Test
//    void test() {
//        setExpression("\"Hello \\\"World\\\"\" == 42");
//    }
//    @Test
//    void test() {
//        setExpression("\"/a b/\\\"c~d\\\"/\\/\"");
//    }
//    @Test
//    void test() {
//        setExpression("1 < 2");
//    }
//    @Test
//    void test() {
//        setExpression("1 < 2 or 3 <= 4 or 5 > 6 or 7 >= 8");
//    }
//    @Test
//    void test() {
//        setExpression("1 in 1");
//    }
//    @Test
//    void test() {
//        setExpression("3 > 1 or (/status_code == 500)");
//    }
//    @Test
//    void test() {
//        setExpression("3>1or(/status_code==500)");
//    }
//    @Test
//    void test() {
//        setExpression("/ABCDEFGHIJKLMNOPQRSTUVWXYZ/ambcdefghijklmnopqrstuvwxyz/0123456789/_");
//    }
//    @Test
//    void test() {
//        setExpression("\"Hello, this \\\"is\\\" a 'complex' ~ string with numbers! 0123\"");
//    }
//    @Test
//    void test() {
//        setExpression("\"foo\"=~\"[A-Z]*\"");
//    }
//    @Test
//    void test() {
//        setExpression("\"foo\"!~\"[A-Z]*\"");
//    }
//    @Test
//    void test() {
//        setExpression("3.14159");
//    }
//    @Test
//    void test() {
//        setExpression("\"foo\"=~3.14");
//    }

    private Executable assertThatHasParseError(final String statement) {
        return () -> {
            final ParserRuleContext expression = parseExpression(statement);
            assertThat("\"" + statement + "\" should have parsing errors", foundErrorNode, is(true));
        };
    }

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
            assertThat("Statement \"" + statement + "\" should have no errors", foundErrorNode, is(false));
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
//        setExpression("\"Hello \\\"world\\\", 'this' is a \\\\ string.\"");
//    }
//    @Test
//    void test() {
//        setExpression("5==");
//    }
//    @Test
//    void test() {
//        setExpression("==");
//    }
//    @Test
//    void test() {
//        setExpression("not (5 not in [1]) or not \"in\"");
//    }
}
