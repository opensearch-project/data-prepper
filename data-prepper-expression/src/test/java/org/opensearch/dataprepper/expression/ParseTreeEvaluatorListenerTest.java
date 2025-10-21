/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ParseTreeEvaluatorListenerTest {
    private final ExpressionFunctionProvider expressionFunctionProvider = mock(ExpressionFunctionProvider.class);
    private final Random random = new Random();
    private final ParseTreeWalker walker = new ParseTreeWalker();
    private final ParseTreeParser parseTreeParser = constructParseTreeParser();
    private final OperatorConfiguration operatorConfiguration = new OperatorConfiguration();
    private final LiteralTypeConversionsConfiguration literalTypeConversionsConfiguration = new LiteralTypeConversionsConfiguration();
    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    private final ParseTreeCoercionService coercionService = new ParseTreeCoercionService(
            literalTypeConversionsConfiguration.literalTypeConversions(), expressionFunctionProvider, eventKeyFactory);
    private final List<Operator<?>> operators = Arrays.asList(
            new AndOperator(), new OrOperator(),
            operatorConfiguration.inSetOperator(), operatorConfiguration.notInSetOperator(),
            operatorConfiguration.equalOperator(), operatorConfiguration.notEqualOperator(operatorConfiguration.equalOperator()),
            operatorConfiguration.greaterThanOperator(), operatorConfiguration.greaterThanOrEqualOperator(),
            operatorConfiguration.lessThanOperator(), operatorConfiguration.lessThanOrEqualOperator(),
            operatorConfiguration.regexEqualOperator(), operatorConfiguration.regexNotEqualOperator(),
            operatorConfiguration.typeOfOperator(),
            operatorConfiguration.addOperator(),
            operatorConfiguration.subtractOperator(),
            operatorConfiguration.multiplyOperator(),
            operatorConfiguration.divideOperator(),
            operatorConfiguration.modOperator(),
            new NotOperator()
    );
    private final OperatorProvider operatorProvider = new OperatorProvider(operators);
    private ParseTreeEvaluatorListener objectUnderTest;

    private ParseTreeParser constructParseTreeParser() {
        final DataPrepperExpressionParser expressionParser = new ParseTreeParserConfiguration().dataPrepperExpressionParser();
        return new ParseTreeParser(expressionParser);
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
    void testVisitErrorNode() {
        final ErrorNode errorNode = mock(ErrorNode.class);
        final Event testEvent = createTestEvent(new HashMap<>());
        objectUnderTest = createObjectUnderTest(testEvent);

        assertThrows(RuntimeException.class, () -> objectUnderTest.visitErrorNode(errorNode));
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
        final String testStringValue = "test string";
        final String testSingleStringStatement = String.format("\"%s\"", testStringValue);
        final Integer testInteger = random.nextInt(1000);
        final String testSingleIntegerStatement = String.format("%d", testInteger);
        final Float testFloat = random.nextFloat();
        final String testSingleFloatStatement = String.valueOf(testFloat);
        final String testSingleBooleanStatement = "true";
        final String testSingleNullStatement = "null";
        final String testKey = "testKey";
        final Integer testValue = random.nextInt();
        final Map<String, Integer> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String testSingleJsonPointerStatement = String.format("/%s", testKey);
        final String testSingleEscapeJsonPointerStatement = String.format("\"/%s\"", testKey);

        assertThat(evaluateStatementOnEvent(testSingleStringStatement, testEvent), equalTo(testStringValue));
        assertThat(evaluateStatementOnEvent(testSingleIntegerStatement, testEvent), equalTo(testInteger));
        assertThat(evaluateStatementOnEvent(testSingleFloatStatement, testEvent), equalTo(testFloat));
        assertThat(evaluateStatementOnEvent(testSingleBooleanStatement, testEvent), equalTo(true));
        assertThat(evaluateStatementOnEvent(testSingleNullStatement, testEvent), equalTo(null));
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
    void testSimpleEqualityOperatorExpressionWithJsonPointerType() {
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
    void testSimpleEqualityOperatorExpressionWithFunctionType() {
        final String testKey = RandomStringUtils.randomAlphabetic(5);
        final String testValue = RandomStringUtils.randomAlphabetic(10);
        final Map<String, String> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        when(expressionFunctionProvider.provideFunction(eq("length"), any(List.class), any(Event.class), any(Function.class))).thenReturn(testValue.length());
        String equalStatement = String.format("length(/%s) == %d", testKey, testValue.length());
        String notEqualStatement = String.format("length(/%s) != %d", testKey, testValue.length() + 1);
        assertThat(evaluateStatementOnEvent(equalStatement, testEvent), is(true));
        assertThat(evaluateStatementOnEvent(notEqualStatement, testEvent), is(true));
        equalStatement = String.format("length(\"%s\") == %d", testValue, testValue.length());
        notEqualStatement = String.format("length(\"%s\") != %d", testValue, testValue.length() + 1);
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
        assertThat(evaluateStatementOnEvent(greaterThanStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(greaterThanOrEqualStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(lessThanStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(lessThanOrEqualStatement, testEvent), is(false));
    }

    @Test
    void testSimpleRelationalOperatorExpressionWithJsonPointerTypeValidValue() {
        final String testKey = "testKey";
        final int testValue = random.nextInt(1000) + 2;
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
    void testSimpleRelationalOperatorExpressionWithJsonPointerTypeInValidValueWithPositiveInteger() {
        final String testKey = "testKey";
        final boolean testValue = true;
        final Map<String, Boolean> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        final String greaterThanStatement = String.format(" /%s > %s", testKey, testValue);
        final String greaterThanOrEqualStatement = String.format(" /%s >= /%s", testKey, testKey);
        final String lessThanStatement = String.format(" /%s < %s", testKey, testValue);
        final String lessThanOrEqualStatement = String.format(" /%s <= /%s", testKey, testKey);
        assertThat(evaluateStatementOnEvent(greaterThanStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(greaterThanOrEqualStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(lessThanStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(lessThanOrEqualStatement, testEvent), is(false));
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
        assertThat(evaluateStatementOnEvent(andStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(orStatement, testEvent), is(false));
    }

    @ParameterizedTest
    @MethodSource("invalidExpressionArguments")
    void testSimpleConditionalOperatorExpressionWithInValidExpression(final String statement) {
        //final String statement = "((/field1 + 10) - (( 2 + /field2 ) + 5) - 10)";
        final Event testEvent = createTestEvent(new HashMap<>());
        assertThrows(ExpressionEvaluationException.class, () -> evaluateStatementOnEvent(statement, testEvent));
    }

    private static Stream<Arguments> invalidExpressionArguments() {
        return Stream.of(
                Arguments.of("(/field1 + 10) + ( 2 + /field2 )"),
                Arguments.of("(/field1 * 10) * ( 2 * /field2 )"),
                Arguments.of("(/field1 / 10) + ( 2 / /field2 )"),
                Arguments.of("(/field1 - 10) - ( 2 - /field2 )")
        );
    }

    @ParameterizedTest
    @MethodSource("booleanExpressionsGeneratingExceptions")
    void testSimpleConditionalOperatorExpressionWithMissingField(final String statement, final boolean expectedResult) {
        final String testKey = "testKey";
        final int testValue = 1;
        final Map<String, Object> data = Map.of(testKey, testValue);
        final Event testEvent = createTestEvent(data);
        assertThat(evaluateStatementOnEvent(statement, testEvent), is(expectedResult));
    }

    private static Stream<Arguments> booleanExpressionsGeneratingExceptions() {
        return Stream.of(
                Arguments.of( "(/field1 > 1) or (/field2 > 2)", false),
                Arguments.of( "(/field1 < 1) or (/field2 < 2)", false),
                Arguments.of( "(/testKey < \"two\") or (2 == 2)", true),
                Arguments.of( "(/testKey < \"two\") or (2)", false),
                Arguments.of( "(/testKey) or (2 == 2)", false),
                Arguments.of( "not (/testKey)", false),
                Arguments.of( "(/testKey < \"two\") and (2)", false),
                Arguments.of( "(/testKey) and (2 == 2)", false),
                Arguments.of( "(/testKey < \"two\") and (2 == 2)", false)
        );
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
        assertThat(evaluateStatementOnEvent(andStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(orStatement, testEvent), is(false));
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
        assertThat(evaluateStatementOnEvent(notStatement, testEvent), is(false));
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
        assertThat(evaluateStatementOnEvent(notStatement, testEvent), is(false));
    }

    @Test
    void testSimpleAddOperatorExpressionWithInteger() {
        final String integerKey1 = "integerKey1";
        final String integerKey2 = "integerKey2";
        final Map<String, Integer> data = Map.of(integerKey1, 1, integerKey2, 2);
        final String addStatement = String.format("/%s + /%s", integerKey1, integerKey2);
        final Event testEvent = createTestEvent(data);
        assertThat(evaluateStatementOnEvent(addStatement, testEvent), equalTo(3));
    }

    @Test
    void testSimpleAddOperatorExpressionWithString() {
        final String stringKey1 = "stringKey1";
        final String stringKey2 = "stringKey2";
        final Map<String, String> data = Map.of(stringKey1, "a", stringKey2, "b");
        final String addStatement = String.format("/%s + /%s", stringKey1, stringKey2);
        final Event testEvent = createTestEvent(data);
        assertThat(evaluateStatementOnEvent(addStatement, testEvent), equalTo("ab"));
    }

    @Test
    void testMultipleOperatorsExpressionNotPriorToRelational() {
        final Event testEvent = createTestEvent(new HashMap<>());
        final String notPriorToGreaterThanStatement = "not 1 > 2";
        final String notPriorToGreaterThanOrEqualStatement = "not 1 >= 1";
        final String notPriorToLessThanStatement = "not 2 < 1";
        final String notPriorToLessThanOrEqualStatement = "not 1 <= 1";
        assertThat(evaluateStatementOnEvent(notPriorToGreaterThanStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(notPriorToGreaterThanOrEqualStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(notPriorToLessThanStatement, testEvent), is(false));
        assertThat(evaluateStatementOnEvent(notPriorToLessThanOrEqualStatement, testEvent), is(false));
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
