/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionLexer;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParseTreeEvaluatorListenerTest {
    private static final CharStream EMPTY_STREAM = CharStreams.fromString("");

    private final Random random = new Random();
    private final ParseTreeWalker walker = new ParseTreeWalker();
    private final ParseTreeParser parseTreeParser = constructParseTreeParser();
    private final OperatorFactory operatorFactory = new OperatorFactory();
    private final ParseTreeCoercionService coercionService = new ParseTreeCoercionService();
    private final List<Operator<?>> operators = Arrays.asList(
            new AndOperator(), new OrOperator(),
            operatorFactory.inSetOperator(), operatorFactory.notInSetOperator(),
            operatorFactory.equalOperator(), operatorFactory.notEqualOperator(),
            operatorFactory.greaterThanOperator(), operatorFactory.greaterThanOrEqualOperator(),
            operatorFactory.lessThanOperator(), operatorFactory.lessThanOrEqualOperator(),
            operatorFactory.regexEqualOperator(), operatorFactory.regexNotEqualOperator(),
            new NotOperator()
    );
    private final OperatorProvider operatorProvider = new OperatorProvider(operators);
    private ParseTreeEvaluatorListener objectUnderTest;

    private ParseTreeParser constructParseTreeParser() {
        final DataPrepperExpressionLexer lexer = new DataPrepperExpressionLexer(EMPTY_STREAM);
        final CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        final DataPrepperExpressionParser expressionParser = new DataPrepperExpressionParser(tokenStream);
        final ParserErrorListener parserErrorListener = new ParserErrorListener(expressionParser);
        return new ParseTreeParser(expressionParser, parserErrorListener);
    }

    private ParseTreeEvaluatorListener createObjectUnderTest(final Event event) {
        return new ParseTreeEvaluatorListener(operatorProvider, coercionService, event);
    }

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    private Object evaluateStatementOnEvent(final String statement, final Event event) {
        final ParseTree parseTree = parseTreeParser.parse(statement);
        objectUnderTest = createObjectUnderTest(event);
        walker.walk(objectUnderTest, parseTree);
        return objectUnderTest.getResult();
    }

    @Test
    void testGetResultWithDoubleWalk() {
        final ParseTree testParseTree = parseTreeParser.parse("true");
        final Event testEvent = createTestEvent(new HashMap<>());
        objectUnderTest = createObjectUnderTest(testEvent);
        walker.walk(objectUnderTest, testParseTree);
        walker.walk(objectUnderTest, testParseTree);
        assertThrows(IllegalStateException.class, objectUnderTest::getResult);
    }

    @Test
    void testSinglePrimaryExpression() {
        final String testSingleStringStatement = "\"test string\"";
        final Integer testInteger = random.nextInt(1000);
        final String testSingleIntegerStatement = String.format("%d", testInteger);
        final Float testFloat = random.nextFloat();
        final String testSingleFloatStatement = String.valueOf(testFloat);
        final String testSingleBooleanStatement = "true";
        final String testKey = "testKey";
        final Integer testValue = random.nextInt();
        final Map<String, Integer> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String testSingleJsonPointerStatement = String.format("/%s", testKey);
        final String testSingleEscapeJsonPointerStatement = String.format("\"/%s\"", testKey);

        assertThat(evaluateStatementOnEvent(testSingleStringStatement, testEvent), equalTo(testSingleStringStatement));
        assertThat(evaluateStatementOnEvent(testSingleIntegerStatement, testEvent), equalTo(testInteger));
        assertThat(evaluateStatementOnEvent(testSingleFloatStatement, testEvent), equalTo(testFloat));
        assertThat(evaluateStatementOnEvent(testSingleBooleanStatement, testEvent), equalTo(true));
        assertThat(evaluateStatementOnEvent(testSingleJsonPointerStatement, testEvent), equalTo(testValue));
        assertThat(evaluateStatementOnEvent(testSingleEscapeJsonPointerStatement, testEvent), equalTo(testValue));
    }

    @Test
    void testSimpleEqualityOperatorExpressionWithLiteralType() {
        final String equalStatement = "\"a\" == \"a\"";
        final String notEqualStatement = "\"a\" != \"b\"";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThat(evaluateStatementOnEvent(equalStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(notEqualStatement, testEvent), is(true));
    }

    @Test
    void testSimpleEqualityOperatorExpressionWithJsonPointerTypeExistingKey() {
        final String testKey = "testKey";
        final Integer testValue = random.nextInt(1000);
        final Map<String, Integer> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String equalStatement = String.format("/%s == %d", testKey, testValue);
        final String notEqualStatement = String.format("/%s != %d", testKey, testValue + 1);
        assertThat(evaluateStatementOnEvent(equalStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(notEqualStatement, testEvent), is(true));
    }

    @Test
    void testSimpleEqualityOperatorExpressionWithJsonPointerTypeMissingKey() {
        final String testMissingKey1 = "missingKey1";
        final String testMissingKey2 = "missingKey2";
        final String equalStatement = String.format("/%s == /%s", testMissingKey1, testMissingKey2);
        final String notEqualStatement = String.format("/%s != 1", testMissingKey1);
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThat(evaluateStatementOnEvent(equalStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(notEqualStatement, testEvent), is(true));
    }

    @Test
    void testSimpleEqualityOperatorExpressionWithEscapeJsonPointerType() {
        final String testKey = "testKey";
        final Integer testValue = random.nextInt(1000);
        final Map<String, Integer> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String equalStatement = String.format("\"/%s\" == %d", testKey, testValue);
        assertThat(evaluateStatementOnEvent(equalStatement, testEvent), is(true));
    }

    @Test
    void testSimpleRelationalOperatorExpressionWithValidLiteralType() {
        final String greaterThanStatement = "2 > 1";
        final String greaterThanOrEqualStatement = "1 >= 1";
        final String lessThanStatement = "1 < 2";
        final String lessThanOrEqualStatement = "1 <= 1";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThat(evaluateStatementOnEvent(greaterThanStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(greaterThanOrEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanOrEqualStatement, testEvent), is(true));
    }

    @Test
    void testSimpleRelationalOperatorExpressionWithInValidLiteralType() {
        final String greaterThanStatement = "2 > true";
        final String greaterThanOrEqualStatement = "1 >= true";
        final String lessThanStatement = "1 < true";
        final String lessThanOrEqualStatement = "1 <= true";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(greaterThanStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(greaterThanOrEqualStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(lessThanStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(lessThanOrEqualStatement, testEvent));
    }

    @Test
    void testSimpleRelationalOperatorExpressionWithJsonPointerTypeValidValue() {
        final String testKey = "testKey";
        final int testValue = random.nextInt(1000);
        final Map<String, Integer> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String greaterThanStatement = String.format(" /%s > %d", testKey, testValue - 1);
        final String greaterThanOrEqualStatement = String.format(" /%s >= /%s", testKey, testKey);
        final String lessThanStatement = String.format(" /%s < %d", testKey, testValue + 1);
        final String lessThanOrEqualStatement = String.format(" /%s <= /%s", testKey, testKey);
        assertThat(evaluateStatementOnEvent(greaterThanStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(greaterThanOrEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanOrEqualStatement, testEvent), is(true));
    }

    @Test
    void testSimpleRelationalOperatorExpressionWithJsonPointerTypeInValidValue() {
        final String testKey = "testKey";
        final boolean testValue = true;
        final Map<String, Boolean> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String greaterThanStatement = String.format(" /%s > %s", testKey, testValue);
        final String greaterThanOrEqualStatement = String.format(" /%s >= /%s", testKey, testKey);
        final String lessThanStatement = String.format(" /%s < %s", testKey, testValue);
        final String lessThanOrEqualStatement = String.format(" /%s <= /%s", testKey, testKey);
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(greaterThanStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(greaterThanOrEqualStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(lessThanStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(lessThanOrEqualStatement, testEvent));
    }

    @Test
    void testSimpleConditionalOperatorExpressionWithValidLiteralType() {
        final String andStatement = "true and false";
        final String orStatement = "true or false";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThat(evaluateStatementOnEvent(andStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(orStatement, testEvent), is(true));
    }

    @Test
    void testSimpleConditionalOperatorExpressionWithInValidLiteralType() {
        final String andStatement = "1 and false";
        final String orStatement = "true or 0";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(andStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(orStatement, testEvent));
    }

    @Test
    void testSimpleConditionalOperatorExpressionWithJsonPointerTypeValidValue() {
        final String testKey = "testKey";
        final boolean testValue = true;
        final Map<String, Boolean> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String andStatement = String.format("/%s and false", testKey);
        final String orStatement = String.format("/%s or false", testKey);
        assertThat(evaluateStatementOnEvent(andStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(orStatement, testEvent), is(true));
    }

    @Test
    void testSimpleConditionalOperatorExpressionWithJsonPointerTypeInValidValue() {
        final String testKey = "testKey";
        final int testValue = random.nextInt(1000);
        final Map<String, Integer> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String andStatement = String.format("/%s and false", testKey);
        final String orStatement = String.format("/%s or false", testKey);
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(andStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(orStatement, testEvent));
    }

    @Test
    void testSimpleNotOperatorExpressionWithValidLiteralType() {
        final String notStatement = "not false";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThat(evaluateStatementOnEvent(notStatement, testEvent), is(true));
    }

    @Test
    void testSimpleNotOperatorExpressionWithInValidLiteralType() {
        final String notStatement = "not 1";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(notStatement, testEvent));
    }

    @Test
    void testSimpleNotOperatorExpressionWithJsonPointerTypeValidValue() {
        final String testKey = "testKey";
        final boolean testValue = false;
        final Map<String, Boolean> data = Map.of(testKey, testValue);
        final String notStatement = String.format("not /%s", testKey);
        final Event testEvent = createTestEvent(data);
        assertThat(evaluateStatementOnEvent(notStatement, testEvent), is(true));
    }

    @Test
    void testSimpleNotOperatorExpressionWithJsonPointerTypeInValidValue() {
        final String testKey = "testKey";
        final int testValue = random.nextInt(1000);
        final Map<String, Integer> data = Map.of(testKey, testValue);
        final String notStatement = String.format("not /%s", testKey);
        final Event testEvent = createTestEvent(data);
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(notStatement, testEvent));
    }

    @Test
    void testMultipleOperatorsExpressionNotPriorToRelational() {
        final Event testEvent = createTestEvent(new HashMap<>());
        final String notPriorToGreaterThanStatement = "not 1 > 2";
        final String notPriorToGreaterThanOrEqualStatement = "not 1 >= 1";
        final String notPriorToLessThanStatement = "not 2 < 1";
        final String notPriorToLessThanOrEqualStatement = "not 1 <= 1";
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(notPriorToGreaterThanStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(notPriorToGreaterThanOrEqualStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(notPriorToLessThanStatement, testEvent));
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(notPriorToLessThanOrEqualStatement, testEvent));
    }

    @Test
    void testMultipleOperatorsExpressionRelationalPriorToEquality() {
        final Event testEvent = createTestEvent(new HashMap<>());
        final String greaterThanPriorToEqualStatement = "2 > 1 == true";
        final String greaterThanOrEqualPriorToEqualStatement = "1 >= 1 == true";
        final String lessThanPriorToEqualStatement = "1 < 2 == true";
        final String lessThanOrEqualPriorToEqualStatement = "1 <= 1 == true";
        assertThat(evaluateStatementOnEvent(greaterThanPriorToEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(greaterThanOrEqualPriorToEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanPriorToEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanOrEqualPriorToEqualStatement, testEvent), is(true));

        final String greaterThanPriorToNotEqualStatement = "2 > 1 != false";
        final String greaterThanOrEqualPriorToNotEqualStatement = "1 >= 1 != false";
        final String lessThanPriorToNotEqualStatement = "1 < 2 != false";
        final String lessThanOrEqualPriorToNotEqualStatement = "1 <= 1 != false";
        assertThat(evaluateStatementOnEvent(greaterThanPriorToNotEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(greaterThanOrEqualPriorToNotEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanPriorToNotEqualStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(lessThanOrEqualPriorToNotEqualStatement, testEvent), is(true));
    }

    @Test
    void testMultipleOperatorsExpressionEqualityPriorToConditional() {
        final Event testEvent = createTestEvent(new HashMap<>());
        final String equalPriorToAndStatement = "true and 1 == 1";
        final String equalPriorToOrStatement = "false or 1 == 1";
        final String notEqualPriorToAndStatement = "true and 1 != 2";
        final String notEqualPriorToOrStatement = "false or 1 != 2";
        assertThat(evaluateStatementOnEvent(equalPriorToAndStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(equalPriorToOrStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(notEqualPriorToAndStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(notEqualPriorToOrStatement, testEvent), is(true));
    }

    @Test
    void testMultipleOperatorsParenthesesExpression() {
        final Event testEvent = createTestEvent(new HashMap<>());
        final String testSingleParenthesisStatement = "not (not false) or true";
        assertThat(evaluateStatementOnEvent(testSingleParenthesisStatement, testEvent), is(true));
        final String testNestedParenthesesStatement = "not ((not false) or true)";
        assertThat(evaluateStatementOnEvent(testNestedParenthesesStatement, testEvent), is(false));
    }
}