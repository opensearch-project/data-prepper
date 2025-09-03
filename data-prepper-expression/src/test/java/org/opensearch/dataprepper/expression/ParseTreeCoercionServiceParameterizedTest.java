/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.dataprepper.expression.antlr.DataPrepperExpressionParser;
import org.opensearch.dataprepper.model.event.Event;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
class ParseTreeCoercionServiceParameterizedTest {

    @Mock
    private TerminalNode terminalNode;

    @Mock
    private Token token;

    private final LiteralTypeConversionsConfiguration literalTypeConversionsConfiguration =
            new LiteralTypeConversionsConfiguration();
    private final ExpressionFunctionProvider expressionFunctionProvider = mock(ExpressionFunctionProvider.class);
    private final ParseTreeCoercionService objectUnderTest = new ParseTreeCoercionService(
            literalTypeConversionsConfiguration.literalTypeConversions(), expressionFunctionProvider);

    @ParameterizedTest
    @MethodSource("provideTerminalNodeTypes")
    void testCoerceTerminalNodeTypes(int tokenType) {
        when(token.getType()).thenReturn(tokenType);
        when(terminalNode.getSymbol()).thenReturn(token);
        final Event testEvent = mock(Event.class);
        
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        
        assertThat(result, equalTo(tokenType));
    }

    private static Stream<Arguments> provideTerminalNodeTypes() {
        return Stream.of(
            Arguments.of(DataPrepperExpressionParser.COMMA),
            Arguments.of(DataPrepperExpressionParser.SET_DELIMITER)
        );
    }

    @ParameterizedTest
    @MethodSource("provideFunctionWithMissingParenthesis")
    void testFunctionWithMissingParenthesis(String functionText, String expectedMessage) {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(functionText);
        final Event testEvent = mock(Event.class);
        
        final ExpressionCoercionException exception = assertThrows(ExpressionCoercionException.class,
                () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent));
        
        assertThat(exception.getMessage(), equalTo(expectedMessage));
    }

    private static Stream<Arguments> provideFunctionWithMissingParenthesis() {
        return Stream.of(
            Arguments.of("lengtharg", "Invalid function format: missing opening parenthesis"),
            Arguments.of("length(arg", "Invalid function format: missing closing parenthesis"),
            Arguments.of("lengtharg)", "Invalid function format: missing opening parenthesis")
        );
    }

    @Test
    void testFunctionWithNoArguments() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("now()");
        final Event testEvent = mock(Event.class);
        when(expressionFunctionProvider.provideFunction(eq("now"), eq(java.util.Collections.<Object>emptyList()), eq(testEvent), any()))
                .thenReturn("2023-01-01");
        
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        
        assertThat(result, equalTo("2023-01-01"));
    }

    @ParameterizedTest
    @MethodSource("provideFunctionErrorCases")
    void testFunctionErrorCases(String functionText, String expectedMessage) {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn(functionText);
        final Event testEvent = mock(Event.class);
        
        final ExpressionCoercionException exception = assertThrows(ExpressionCoercionException.class,
                () -> objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent));
        
        assertThat(exception.getMessage(), equalTo(expectedMessage));
    }

    private static Stream<Arguments> provideFunctionErrorCases() {
        return Stream.of(
            Arguments.of("test(\"unclosed", "Invalid function format: missing closing parenthesis"),
            Arguments.of("test(123)", "Unsupported type passed as function argument")
        );
    }

    @Test
    void testFunctionWithEmptyStringArgument() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("test(\"\")");
        final Event testEvent = mock(Event.class);
        when(expressionFunctionProvider.provideFunction(eq("test"), eq(java.util.List.<Object>of("\"\"")), eq(testEvent), any()))
                .thenReturn("result");
        
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        
        assertThat(result, equalTo("result"));
    }

    @Test
    void testFunctionWithEmptyArgument() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("test(/key, , \"value\")");
        final Event testEvent = mock(Event.class);
        when(expressionFunctionProvider.provideFunction(eq("test"), eq(java.util.List.<Object>of("/key", "\"value\"")), eq(testEvent), any()))
                .thenReturn("result");
        
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        
        assertThat(result, equalTo("result"));
    }

    @Test
    void testFunctionWithMultipleArguments() {
        when(token.getType()).thenReturn(DataPrepperExpressionParser.Function);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("test(/key, \"value\")");
        final Event testEvent = mock(Event.class);
        when(expressionFunctionProvider.provideFunction(eq("test"), eq(java.util.List.<Object>of("/key", "\"value\"")), eq(testEvent), any()))
                .thenReturn("result");
        
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        
        assertThat(result, equalTo("result"));
    }

    @ParameterizedTest
    @MethodSource("provideConstructorNullParameters")
    void testConstructorWithNullParameters(Map<Class<? extends java.io.Serializable>, Function<Object, Object>> conversions, ExpressionFunctionProvider provider) {
        assertThrows(NullPointerException.class, 
                () -> new ParseTreeCoercionService(conversions, provider));
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
        when(testEvent.get("/nonexistent", Object.class)).thenReturn(null);
        
        final Object result = objectUnderTest.coercePrimaryTerminalNode(terminalNode, testEvent);
        
        assertThat(result, equalTo(null));
    }

    @Test
    void testConvertLiteralTypeWithUnsupportedType() {
        // Create a service with limited type conversions to test the error path
        Map<Class<? extends java.io.Serializable>, Function<Object, Object>> limitedConversions = new HashMap<>();
        ParseTreeCoercionService limitedService = new ParseTreeCoercionService(limitedConversions, expressionFunctionProvider);
        
        when(token.getType()).thenReturn(DataPrepperExpressionParser.JsonPointer);
        when(terminalNode.getSymbol()).thenReturn(token);
        when(terminalNode.getText()).thenReturn("/key");
        final Event testEvent = mock(Event.class);
        when(testEvent.get("/key", Object.class)).thenReturn("unsupported");
        
        assertThrows(ExpressionCoercionException.class,
                () -> limitedService.coercePrimaryTerminalNode(terminalNode, testEvent));
    }
}