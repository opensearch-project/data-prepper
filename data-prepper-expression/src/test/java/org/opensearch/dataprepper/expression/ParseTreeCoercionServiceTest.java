/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.expression.util.TestObject;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParseTreeCoercionServiceTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Mock
    private TerminalNode terminalNode;

    @Mock
    private Token token;

    private final LiteralTypeConversionsConfiguration literalTypeConversionsConfiguration =
            new LiteralTypeConversionsConfiguration();
    private final ExpressionFunctionProvider expressionFunctionProvider = mock(ExpressionFunctionProvider.class);
    private final ParseTreeCoercionService objectUnderTest = new ParseTreeCoercionService(
            literalTypeConversionsConfiguration.literalTypeConversions(), expressionFunctionProvider);

    @Test
    void testCoerceTerminalNodeStringType() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.String);
        final String testString = "test string";
        final String testNodeStringValue = String.format("\"%s\"", testString);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(testNodeStringValue);
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(String.class));
        assertThat(result, equalTo(testString));
    }

    @Test
    void testCoerceTerminalNodeIntegerType() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Integer);
        final Integer testInteger = new Random().nextInt();
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(String.valueOf(testInteger));
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(Integer.class));
        assertThat(result, equalTo(testInteger));
    }

    @Test
    void testCoerceTerminalNodeLongType() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Integer);
        final Long testLong = new Random().nextLong() + (long) Integer.MAX_VALUE;
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(String.valueOf(testLong));
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(Long.class));
        assertThat(result, equalTo(testLong));
        when(terminalNode.getText()).thenReturn(String.valueOf(-testLong));
        final Event negativeTestEvent = createTestEvent(new HashMap<>());
        final Object negativeResult = objectUnderTest.coercePrimaryTerminalNode(terminalNode, negativeTestEvent);
        assertThat(negativeResult, instanceOf(Long.class));
        assertThat(negativeResult, equalTo(-testLong));
    }

    @Test
    void testCoerceTerminalNodeFloatType() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Float);
        final Float testFloat = new Random().nextFloat();
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(String.valueOf(testFloat));
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(Float.class));
        assertThat(result, equalTo(testFloat));
    }

    @Test
    void testCoerceTerminalNodeBooleanType() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Boolean);
        final Boolean testBoolean = new Random().nextBoolean();
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(String.valueOf(testBoolean));
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(Boolean.class));
        assertThat(result, equalTo(testBoolean));
    }

    @Test
    void testCoerceTerminalNodeNullType() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Null);
        final Event testEvent = createTestEvent(new HashMap<>());
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(null);
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        assertThat(result, equalTo(null));
    }

    @ParameterizedTest
    @MethodSource("provideSupportedJsonPointerValues")
    void testCoerceTerminalNodeJsonPointerTypeSupportedValues(final Object testValue) {
        final String testKey1 = "key1";
        final String testKey2 = "key2";
        final String testJsonPointerKey = String.format("/%s/%s", testKey1, testKey2);
        final Event testEvent = testValue == null ? createTestEvent(new HashMap<>()) :
                createTestEvent(Map.of(testKey1, Map.of(testKey2, testValue)));
        when(token.getType()).thenReturn(DataPrepperExpressionParser.JsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(testJsonPointerKey);
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        if (testValue instanceof Double) {
            assertThat(result, instanceOf(Float.class));
            assertThat(result, equalTo(((Double) testValue).floatValue()));
        } else {
            assertThat(result, equalTo(testValue));
        }
    }

    @ParameterizedTest
    @MethodSource("provideUnSupportedJsonPointerValues")
    void testCoerceTerminalNodeJsonPointerTypeUnSupportedValues(final Object testValue) {
        final String testKey1 = "key1";
        final String testKey2 = "key2";
        final String testJsonPointerKey = String.format("/%s/%s", testKey1, testKey2);
        final Event testEvent = testValue == null ? createTestEvent(new HashMap<>()) :
                createTestEvent(Map.of(testKey1, Map.of(testKey2, testValue)));
        when(token.getType()).thenReturn(DataPrepperExpressionParser.JsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(testJsonPointerKey);
        assertThrows(ExpressionCoercionException.class, () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent));
    }

    @ParameterizedTest
    @MethodSource("provideKeys")
    void testCoerceTerminalNodeEscapeJsonPointerTypeWithSpecialCharacters(final String testKey, final String testEscapeJsonPointer)
            throws ExpressionCoercionException {
        final String testValue = "test value";
        final Event testEvent = createTestEvent(Map.of(testKey, testValue));
        when(token.getType()).thenReturn(DataPrepperExpressionParser.EscapedJsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(testEscapeJsonPointer);
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(String.class));
        assertThat(result, equalTo(testValue));
    }

    @ParameterizedTest
    @MethodSource("provideSupportedJsonPointerValues")
    void testCoerceTerminalNodeEscapeJsonPointerTypeSupportedValues(final Object testValue) {
        final String testKey = "testKey";
        final String testEscapeJsonPointerKey = String.format("\"/%s\"", testKey);
        final Event testEvent = testValue == null ? createTestEvent(new HashMap<>()) :
                createTestEvent(Map.of(testKey, testValue));
        when(token.getType()).thenReturn(DataPrepperExpressionParser.EscapedJsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(testEscapeJsonPointerKey);
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        if (testValue instanceof Double) {
            assertThat(result, instanceOf(Float.class));
            assertThat(result, equalTo(((Double) testValue).floatValue()));
        } else {
            assertThat(result, equalTo(testValue));
        }
    }

    @ParameterizedTest
    @MethodSource("provideUnSupportedJsonPointerValues")
    void testCoerceTerminalNodeEscapeJsonPointerTypeUnSupportedValues(final Object testValue) {
        final String testKey = "testKey";
        final String testEscapeJsonPointerKey = String.format("\"/%s\"", testKey);
        final Event testEvent = testValue == null ? createTestEvent(new HashMap<>()) :
                createTestEvent(Map.of(testKey, testValue));
        when(token.getType()).thenReturn(DataPrepperExpressionParser.EscapedJsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(testEscapeJsonPointerKey);
        assertThrows(ExpressionCoercionException.class, () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent));
    }

    @Test
    void testCoerceTerminalNodeUnsupportedType() {
        final Event testEvent = createTestEvent(new HashMap<>());
        when(terminalNode.getSymbol()).thenReturn(token);
        when(token.getType()).thenReturn(-1);
        assertThrows(ExpressionCoercionException.class, () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent));
    }

    @Test
    void testCoerceSuccess() throws ExpressionCoercionException {
        final Object testObj = false;
        final Boolean result = objectUnderTest.coerce(testObj, Boolean.class);
        assertThat(result, instanceOf(Boolean.class));
        assertThat(result, is(false));
    }

    @Test
    void testCoerceFailure() {
        final Object testObj = new TestObject("");
        assertThrows(ExpressionCoercionException.class, () -> objectUnderTest.coerce(testObj, String.class));
    }

    @Test
    void testCoerceTerminalNodeLengthFunction() {
        final String key = RandomStringUtils.randomAlphabetic(5);
        final String value = RandomStringUtils.randomAlphabetic(10);
        final Event testEvent = createTestEvent(Map.of(key, value));
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("length(/"+key+")");
        when(expressionFunctionProvider.provideFunction(eq("length"), any(List.class), any(Event.class), any(Function.class))).thenReturn(value.length());
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        assertThat(objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent), equalTo(value.length()));
    }

    @Test
    void testCoerceTerminalNodeLengthFunctionWithInvalidString() {
        final String key = RandomStringUtils.randomAlphabetic(5);
        final String value = RandomStringUtils.randomAlphabetic(10);
        final Event testEvent = createTestEvent(Map.of(key, value));
        final String testString = RandomStringUtils.randomAlphabetic(10);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("length(\""+testString+")");
        when(expressionFunctionProvider.provideFunction(eq("length"), any(List.class), any(Event.class), any(Function.class))).thenReturn(value.length());
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        assertThrows(RuntimeException.class, () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent));
    }

    @Test
    void testCoerceTerminalNodeLengthFunctionWithInvalidArgument() {
        final String key = RandomStringUtils.randomAlphabetic(5);
        final String value = RandomStringUtils.randomAlphabetic(10);
        final Event testEvent = createTestEvent(Map.of(key, value));
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("length(10)");
        when(expressionFunctionProvider.provideFunction(eq("length"), any(List.class), any(Event.class), any(Function.class))).thenReturn(value.length());
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        assertThrows(RuntimeException.class, () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent));
    }

    @Test
    void testCoerceTerminalNodeLengthFunctionKeyNotInEvent() {
        final String key = RandomStringUtils.randomAlphabetic(5);
        final String value = RandomStringUtils.randomAlphabetic(10);
        final String key2 = RandomStringUtils.randomAlphabetic(5);
        final Event testEvent = createTestEvent(Map.of(key, value));
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("length(/"+key2+")");
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        assertThat(objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent), equalTo(null));
    }

    @Test
    void testCoerceTerminalNodeWithUnknownFunction() {
        when(terminalNode.getSymbol()).thenReturn(token);
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        when(terminalNode.getText()).thenReturn("xyz(arg1)");
        assertThrows(RuntimeException.class, () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, null));
    }

    private Event createTestEvent(final Object data) {
        final Event event = mock(Event.class);
        final JsonNode node = mapper.valueToTree(data);
        lenient().when(event.get(anyString(), any())).thenAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            final String jsonPointer = (String) args[0];
            final Class<?> clazz = (Class<?>) args[1];
            final JsonNode childNode = node.at(jsonPointer);
            if (childNode.isMissingNode()) {
                return null;
            }
            return mapper.treeToValue(childNode, clazz);
        });
        return event;
    }

    private static Stream<Arguments> provideKeys() {
        return Stream.of(
                Arguments.of("test key", "\"/test key\""),
                Arguments.of("test/key", "\"/test~1key\""),
                Arguments.of("test\\key", "\"/test\\key\""),
                Arguments.of("test~0key", "\"/test~00key\"")
        );
    }

    private static Stream<Arguments> provideSupportedJsonPointerValues() {
        return Stream.of(
                Arguments.of(1000),
                Arguments.of(1234512345000L),
                Arguments.of(true),
                Arguments.of("test value"),
                Arguments.of(1.1f),
                Arguments.of(1.1),
                Arguments.of((Object) null)
        );
    }

    private static Stream<Arguments> provideUnSupportedJsonPointerValues() {
        return Stream.of(Arguments.of(new HashMap<>()));
    }
}
