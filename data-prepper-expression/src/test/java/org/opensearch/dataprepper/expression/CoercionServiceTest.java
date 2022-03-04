/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CoercionServiceTest {

    @Mock
    private TerminalNode terminalNode;

    @Mock
    private Token token;

    private final CoercionService objectUnderTest = new CoercionService();

    @BeforeEach
    void setUp() {
        lenient().when(terminalNode.getSymbol()).thenReturn(token);
    }

    @Test
    void testCoerceTerminalNodeStringType() throws CoercionException {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.String);
        final String testString = "test string";
        when(terminalNode.getText()).thenReturn(testString);
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coerceTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(String.class));
        assertThat(result, equalTo(testString));
    }

    @Test
    void testCoerceTerminalNodeIntegerType() throws CoercionException {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Integer);
        final Integer testInteger = 1;
        when(terminalNode.getText()).thenReturn(String.valueOf(testInteger));
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coerceTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(Integer.class));
        assertThat(result, equalTo(testInteger));
    }

    @Test
    void testCoerceTerminalNodeFloatType() throws CoercionException {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Float);
        final Float testFloat = 1.0f;
        when(terminalNode.getText()).thenReturn(String.valueOf(testFloat));
        final Event testEvent = createTestEvent(new HashMap<>());
        final Object result = objectUnderTest.coerceTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(Float.class));
        assertThat(result, equalTo(testFloat));
    }

    @Test
    void testCoerceTerminalNodeJsonPointerType() throws CoercionException {
        final String testKey1 = "key1";
        final String testKey2 = "key2";
        final String testValue = "value";
        final String testJsonPointerKey = String.format("/%s/%s", testKey1, testKey2);
        final Event testEvent = createTestEvent(Map.of(testKey1, Map.of(testKey2, testValue)));
        when(token.getType()).thenReturn(DataPrepperExpressionParser.JsonPointer);
        when(terminalNode.getText()).thenReturn(testJsonPointerKey);
        final Object result = objectUnderTest.coerceTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(String.class));
        assertThat(result, equalTo(testValue));
    }

    @Test
    void testCoerceTerminalNodeJsonPointerTypeMissingKey() throws CoercionException {
        final String testMissingKey = "missingKey";
        final Event testEvent = createTestEvent(new HashMap<>());
        when(token.getType()).thenReturn(DataPrepperExpressionParser.JsonPointer);
        when(terminalNode.getText()).thenReturn(testMissingKey);
        final Object result = objectUnderTest.coerceTerminalNode(terminalNode, testEvent);
        assertThat(result, nullValue());
    }

    @Test
    void testCoerceTerminalNodeEscapedJsonPointerType() throws CoercionException {
        final String testKey = "key";
        final String testValue = "value";
        final String testEscapedJsonPointerKey = String.format("//%s", testKey);
        final Event testEvent = createTestEvent(Map.of(testKey, testValue));
        when(token.getType()).thenReturn(DataPrepperExpressionParser.EscapedJsonPointer);
        when(terminalNode.getText()).thenReturn(testEscapedJsonPointerKey);
        final Object result = objectUnderTest.coerceTerminalNode(terminalNode, testEvent);
        assertThat(result, instanceOf(String.class));
        assertThat(result, equalTo(testEscapedJsonPointerKey));
    }

    @Test
    void testCoerceTerminalNodeEscapedUnsupportedType() {
        final Event testEvent = createTestEvent(new HashMap<>());
        when(token.getType()).thenReturn(-1);
        assertThrows(CoercionException.class, () -> objectUnderTest.coerceTerminalNode(terminalNode, testEvent));
    }

    @Test
    void testCoerceSuccess() throws CoercionException {
        final Object testObj = false;
        final Boolean result = objectUnderTest.coerce(testObj, Boolean.class);
        assertThat(result, instanceOf(Boolean.class));
        assertThat(result, is(false));
    }

    @Test
    void testCoerceFailure() {
        final Object testObj = false;
        assertThrows(CoercionException.class, () -> objectUnderTest.coerce(testObj, String.class));
    }

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }
}