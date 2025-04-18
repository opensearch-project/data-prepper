/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.apache.commons.lang3.RandomStringUtils;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

class GetMetadataExpressionFunctionTest {
    private GetMetadataExpressionFunction getMetadataExpressionFunction;
    private Event testEvent;
    private Function<Object, Object> testFunction;

    private Event createTestEvent(final Object data) {
        final Map<String, Object> attributesMap = Map.of("key1", "value3", "key2", 2000, "key3", 12345.6789);
        return JacksonEvent.builder().withEventType("event").withData(data).withEventMetadataAttributes(attributesMap).build();
    }

    public GetMetadataExpressionFunction createObjectUnderTest() {
        testFunction = mock(Function.class);
        testEvent = createTestEvent(Map.of());
        return new GetMetadataExpressionFunction();
    }

    private static Stream<Arguments> getAttributeTestInputs() {
        return Stream.of(Arguments.of("key1", "value3"),
                         Arguments.of("key2", 2000),
                         Arguments.of("key3", 12345.6789),
                         Arguments.of("/key1", "value3"),
                         Arguments.of("/key2", 2000),
                         Arguments.of("/key3", 12345.6789));
    }

    @ParameterizedTest
    @MethodSource("getAttributeTestInputs")
    void testGetMetadataBasic(final String key, final Object value) {
        getMetadataExpressionFunction = createObjectUnderTest();
        when(testFunction.apply(value)).thenReturn(value);
        assertThat(getMetadataExpressionFunction.evaluate(List.of("\""+key+"\""), testEvent, testFunction), equalTo(value));
        assertThat(getMetadataExpressionFunction.evaluate(List.of("\""+key+"notPresent\""), testEvent, testFunction), equalTo(null));
    }

    @Test
    void testGetMetadataWithMoreArguments() {
        getMetadataExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(5);
        assertThrows(IllegalArgumentException.class, () -> getMetadataExpressionFunction.evaluate(List.of("arg1", "arg2"), testEvent, testFunction));
        assertThrows(IllegalArgumentException.class, () -> getMetadataExpressionFunction.evaluate(List.of("\"arg1\"", "\"arg2\""), testEvent, testFunction));
        assertThrows(IllegalArgumentException.class, () -> getMetadataExpressionFunction.evaluate(List.of("arg1", "arg2", "arg3/arg4"), testEvent, testFunction));
    }

    @Test
    void testGetMetadataWithNonStringArguments() {
        getMetadataExpressionFunction = createObjectUnderTest();
        assertThrows(IllegalArgumentException.class, () -> getMetadataExpressionFunction.evaluate(List.of(10), testEvent, testFunction));
    }

    @Test
    void testGetMetadataWithNonLiteralStringArguments() {
        getMetadataExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(5);
        assertThrows(IllegalArgumentException.class, () -> getMetadataExpressionFunction.evaluate(List.of("testString"), testEvent, testFunction));
    }

    @Test
    void testGetMetadataWithInvalidArguments() {
        getMetadataExpressionFunction = createObjectUnderTest();
        assertThrows(IllegalArgumentException.class, () -> getMetadataExpressionFunction.evaluate(List.of(), testEvent, testFunction));
        assertThrows(IllegalArgumentException.class, () -> getMetadataExpressionFunction.evaluate(List.of("\""), testEvent, testFunction));
    }

    @Test
    void testGetMetadataWithEmptyStringArgument() {
        getMetadataExpressionFunction = createObjectUnderTest();
        assertThat(getMetadataExpressionFunction.evaluate(List.of("  "), testEvent, testFunction), equalTo(null));
        assertThat(getMetadataExpressionFunction.evaluate(List.of("\"\""), testEvent, testFunction), equalTo(null));
    }


}
