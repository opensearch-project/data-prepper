/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.lang3.RandomStringUtils;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.function.Function;

class ContainsSubstringExpressionFunctionTest {
    private ContainsSubstringExpressionFunction containsSubstringExpressionFunction;
    private Event testEvent;
    private Function<Object, Object> testFunction;
    private List<Object> tags;
    private String testKey;
    private String testKey2;
    private String testKey3;
    private String testValue;
    private String testValue2;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    @BeforeEach
    public void setUp() {
        testKey = RandomStringUtils.randomAlphabetic(5);
        testKey2 = RandomStringUtils.randomAlphabetic(5);
        testKey3 = RandomStringUtils.randomAlphabetic(5);
        testValue = RandomStringUtils.randomAlphabetic(10);
        testValue2 = testValue.substring(2,6);
        testEvent = createTestEvent(Map.of(testKey, testValue, testKey2, testValue2, testKey3, 1234));
        tags = new ArrayList<>();
        testFunction = mock(Function.class);
    }

    public ContainsSubstringExpressionFunction createObjectUnderTest() {
        return new ContainsSubstringExpressionFunction();
    }

    @Test
    void testContainsSubstringBasic() {
        containsSubstringExpressionFunction = createObjectUnderTest();
        assertThat(containsSubstringExpressionFunction.evaluate(List.of("\"abcde\"", "\"abcd\""), testEvent, testFunction), equalTo(true));
        assertThat(containsSubstringExpressionFunction.evaluate(List.of("/"+testKey, "/"+testKey2), testEvent, testFunction), equalTo(true));
        assertThat(containsSubstringExpressionFunction.evaluate(List.of("\""+testValue+"\"", "/"+testKey2), testEvent, testFunction), equalTo(true));
        assertThat(containsSubstringExpressionFunction.evaluate(List.of("/"+testKey, "\""+testValue2+"\""), testEvent, testFunction), equalTo(true));
        assertThat(containsSubstringExpressionFunction.evaluate(List.of("\"abcde\"", "\"xyz\""), testEvent, testFunction), equalTo(false));
        assertThat(containsSubstringExpressionFunction.evaluate(List.of("/unknown", "xyz"), testEvent, testFunction), equalTo(false));
        assertThat(containsSubstringExpressionFunction.evaluate(List.of("\"abcde\"", "/unknown"), testEvent, testFunction), equalTo(false));
    }

    @Test
    void testContainsSubstringMultipleSubstrings() {
        containsSubstringExpressionFunction = createObjectUnderTest();
        String testString = RandomStringUtils.randomAlphabetic(10);
        for (int i = 1; i < testString.length() - 1; i++) {
            assertThat(containsSubstringExpressionFunction.evaluate(List.of("\""+testString+"\"", "\""+testString.substring(0, i)+"\""), testEvent, testFunction), equalTo(true));
        }
    }

    @Test
    void testInvalidContainsSubstring() {
        containsSubstringExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> containsSubstringExpressionFunction.evaluate(List.of("abcd"), testEvent, testFunction));
        assertThrows(RuntimeException.class, () -> containsSubstringExpressionFunction.evaluate(List.of("\"abcd\"", 1234), testEvent, testFunction));
        assertThrows(RuntimeException.class, () -> containsSubstringExpressionFunction.evaluate(List.of("\"abcd\"", "/"+testKey3), testEvent, testFunction));
        assertThrows(RuntimeException.class, () -> containsSubstringExpressionFunction.evaluate(List.of("abcd", "/"+testKey3), testEvent, testFunction));
    }

}

