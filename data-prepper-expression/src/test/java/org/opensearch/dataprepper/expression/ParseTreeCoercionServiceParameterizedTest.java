/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParseTreeCoercionServiceParameterizedTest {

    @Mock
    private TerminalNode terminalNode;

    @Mock
    private Token token;

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    private final LiteralTypeConversionsConfiguration literalTypeConversionsConfiguration =
            new LiteralTypeConversionsConfiguration();
    private final ExpressionFunctionProvider expressionFunctionProvider = mock(ExpressionFunctionProvider.class);
    private final ParseTreeCoercionService objectUnderTest = new ParseTreeCoercionService(
            literalTypeConversionsConfiguration.literalTypeConversions(), expressionFunctionProvider, eventKeyFactory);

    @Test
    void testCoerceCommaTerminalNodeType() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.COMMA);
        when(terminalNode.getSymbol()).thenReturn(token);
        final Event testEvent = mock(Event.class);

        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);

        assertThat(result, equalTo(DataPrepperExpressionParser.COMMA));
    }

    @Test
    void testEvaluateFunction() {
        final Event testEvent = mock(Event.class);
        final List<Object> args = List.of("arg1");
        when(expressionFunctionProvider.provideFunction(eq("testFunc"), eq(args), eq(testEvent), any()))
                .thenReturn("result");

        final Object result = objectUnderTest.evaluateFunction("testFunc", args, testEvent);

        assertThat(result, equalTo("result"));
    }

    @Test
    void testCreateEventKey() {
        final EventKey result = objectUnderTest.createEventKey("/testKey");
        assertThat(result.getKey(), equalTo("/testKey"));
    }

    @ParameterizedTest
    @MethodSource("provideConstructorNullParameters")
    void testConstructorWithNullParameters(Map<Class<? extends java.io.Serializable>, Function<Object, Object>> conversions, ExpressionFunctionProvider provider) {
        assertThrows(NullPointerException.class,
                () -> new ParseTreeCoercionService(conversions, provider, eventKeyFactory));
    }

    private static Stream<Arguments> provideConstructorNullParameters() {
        ExpressionFunctionProvider mockProvider = mock(ExpressionFunctionProvider.class);
        LiteralTypeConversionsConfiguration config = new LiteralTypeConversionsConfiguration();
        return Stream.of(
            Arguments.of(null, mockProvider),
            Arguments.of(config.literalTypeConversions(), null)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"test\"", "\"\"\"test\"\"\"", "\"test with spaces\""})
    void testStringTypeWithDifferentQuotePatterns(String input) {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.String);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(input);
        final Event testEvent = mock(Event.class);

        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);

        // Should strip quotes
        String expected = input.replaceAll("^\"{1,3}|\"{1,3}$", "");
        assertThat(result, equalTo(expected));
    }

    @Test
    void testJsonPointerWithNullValue() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.JsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("/nonexistent");
        final Event testEvent = mock(Event.class);
        when(testEvent.get(any(EventKey.class), eq(Object.class))).thenReturn(null);

        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);

        assertThat(result, equalTo(null));
    }

    @Test
    void testConvertLiteralTypeWithUnsupportedType() {
        Map<Class<? extends java.io.Serializable>, Function<Object, Object>> limitedConversions = new HashMap<>();
        ParseTreeCoercionService limitedService =
                new ParseTreeCoercionService(limitedConversions, expressionFunctionProvider, eventKeyFactory);

        when(token.getType()).thenReturn(DataPrepperExpressionParser.JsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("/key");
        final Event testEvent = mock(Event.class);
        when(testEvent.get(any(EventKey.class), eq(Object.class))).thenReturn("unsupported");

        assertThrows(ExpressionCoercionException.class,
                () -> limitedService.coercePrimaryTerminalNode(terminalNode, testEvent));
    }
}
