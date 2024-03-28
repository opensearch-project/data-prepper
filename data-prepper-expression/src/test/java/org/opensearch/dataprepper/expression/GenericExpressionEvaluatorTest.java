/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class GenericExpressionEvaluatorTest {

    @Mock
    private Parser<ParseTree> parser;
    @Mock
    private Evaluator<ParseTree, Event> evaluator;
    @InjectMocks
    private GenericExpressionEvaluator statementEvaluator;

    @Test
    void testGivenValidParametersThenEvaluatorResultReturned() {
        final String statement = UUID.randomUUID().toString();
        final ParseTree parseTree = mock(ParseTree.class);
        final Event event = mock(Event.class);
        final String expectedStr = UUID.randomUUID().toString();

        doReturn(parseTree).when(parser).parse(eq(statement));
        doReturn(expectedStr).when(evaluator).evaluate(eq(parseTree), eq(event));

        final Object actualStr = statementEvaluator.evaluate(statement, event);

        assertThat((String)actualStr, is(expectedStr));
        verify(parser).parse(eq(statement));
        verify(evaluator).evaluate(eq(parseTree), eq(event));

        final Random random = new Random();
        final Integer expectedInt = random.nextInt(1000);

        doReturn(parseTree).when(parser).parse(eq(statement));
        doReturn(expectedInt).when(evaluator).evaluate(eq(parseTree), eq(event));

        final Object actualInt = statementEvaluator.evaluate(statement, event);

        assertThat((Integer)actualInt, is(expectedInt));
        verify(parser, times(2)).parse(eq(statement));
        verify(evaluator, times(2)).evaluate(eq(parseTree), eq(event));
    }

    @Test
    void testGivenParserThrowsExceptionThenExceptionThrown() {
        final String statement = UUID.randomUUID().toString();

        doThrow(new RuntimeException()).when(parser).parse(eq(statement));

        assertThrows(ExpressionEvaluationException.class, () -> statementEvaluator.evaluate(statement, null));

        verify(parser).parse(eq(statement));
        verify(evaluator, times(0)).evaluate(any(), any());
    }

    @Test
    void testGivenEvaluatorThrowsExceptionThenExceptionThrown() {
        final String statement = UUID.randomUUID().toString();
        final ParseTree parseTree = mock(ParseTree.class);
        final Event event = mock(Event.class);

        doReturn(parseTree).when(parser).parse(eq(statement));
        doThrow(new RuntimeException()).when(evaluator).evaluate(eq(parseTree), eq(event));

        assertThrows(ExpressionEvaluationException.class, () -> statementEvaluator.evaluateConditional(statement, event));

        verify(parser).parse(eq(statement));
        verify(evaluator).evaluate(eq(parseTree), eq(event));
    }

    @Test
    void isValidExpressionStatement_returns_true_when_parse_does_not_throw() {
        final String statement = UUID.randomUUID().toString();
        final ParseTree parseTree = mock(ParseTree.class);

        doReturn(parseTree).when(parser).parse(eq(statement));

        final boolean result = statementEvaluator.isValidExpressionStatement(statement);

        assertThat(result, equalTo(true));

        verify(parser).parse(eq(statement));
    }

    @Test
    void isValidExpressionStatement_returns_false_when_parse_throws() {
        final String statement = UUID.randomUUID().toString();

        doThrow(RuntimeException.class).when(parser).parse(eq(statement));

        final boolean result = statementEvaluator.isValidExpressionStatement(statement);

        assertThat(result, equalTo(false));
    }

    @ParameterizedTest
    @CsvSource({
            "abc-${/foo, false",
            "abc-${/foo}, true",
            "abc-${getMetadata(\"key\")}, true",
            "abc-${getXYZ(\"key\")}, true",
            "abc-${invalid, false"
    })
    void isValidFormatExpressionsReturnsCorrectResult(final String format, final Boolean expectedResult) {
        assertThat(statementEvaluator.isValidFormatExpression(format), equalTo(expectedResult));
    }

    @ParameterizedTest
    @ValueSource(strings = {"abc-${anyS(=tring}"})
    void isValidFormatExpressionsReturnsFalseWhenIsValidKeyAndValidExpressionIsFalse(final String format) {
        doThrow(RuntimeException.class).when(parser).parse(anyString());
        assertThat(statementEvaluator.isValidFormatExpression(format), equalTo(false));
    }

    @ParameterizedTest
    @ArgumentsSource(FormatExpressionsToExtractedDynamicKeysArgumentProvider.class)
    void extractDynamicKeysFromFormatExpression_returns_expected_result(final String formatExpression, final List<String> expectedDynamicKeys) {
        final List<String> result = statementEvaluator.extractDynamicKeysFromFormatExpression(formatExpression);

        assertThat(result, notNullValue());
        assertThat(result.equals(expectedDynamicKeys), equalTo(true));
    }

    @ParameterizedTest
    @ArgumentsSource(FormatExpressionsToExtractedDynamicExpressionsArgumentProvider.class)
    void extractDynamicExpressionsFromFormatExpression_returns_expected_result(final String formatExpression, final List<String> expectedDynamicExpressions) {
        final List<String> result = statementEvaluator.extractDynamicExpressionsFromFormatExpression(formatExpression);

        assertThat(result, notNullValue());
        assertThat(result.equals(expectedDynamicExpressions), equalTo(true));
    }

    static class FormatExpressionsToExtractedDynamicKeysArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments("test-${foo}-${bar}", List.of("/foo", "/bar")),
                    arguments("test-${getMetadata(\"key\"}-${/test}", List.of("/test")),
                    arguments("test-format", List.of())
            );
        }
    }

    static class FormatExpressionsToExtractedDynamicExpressionsArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments("test-${foo}-${bar}", List.of()),
                    arguments("test-${getMetadata(\"key\")}-${/test}", List.of("getMetadata(\"key\")"))
            );
        }
    }
}

