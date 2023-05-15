/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

class HasTagsExpressionFunctionTest {
    private HasTagsExpressionFunction hasTagsExpressionFunction;
    private Event testEvent;
    private Function<Object, Object> testFunction;
    private List<Object> tags;

    private Event createTestEvent(final Object data) {
        return JacksonEvent.builder().withEventType("event").withData(data).build();
    }

    @BeforeEach
    public void setUp() {
        String key = RandomStringUtils.randomAlphabetic(5);
        String value = RandomStringUtils.randomAlphabetic(10);
        testEvent = createTestEvent(Map.of(key, value));
        tags = new ArrayList<>();
        testFunction = mock(Function.class);
    }

    public void generateTags(Event event, int numTags) {
        for (int i = 0; i < numTags; i++) {
            String tag = RandomStringUtils.randomAlphabetic(5);
            testEvent.getMetadata().addTag(tag);
            tags.add("\""+tag+"\"");
        }
    }

    public HasTagsExpressionFunction createObjectUnderTest() {
        return new HasTagsExpressionFunction();
    }

    @ParameterizedTest
    @ValueSource(ints={1, 2, 10})
    void testHasTagsBasic(int numTags) {
        hasTagsExpressionFunction = createObjectUnderTest();
        generateTags(testEvent, numTags);
        for (int i = 1; i <= numTags; i++) {
            assertThat(hasTagsExpressionFunction.evaluate(tags.subList(0, i), testEvent, testFunction), equalTo(true));
        }
    }

    @ParameterizedTest
    @ValueSource(ints={1, 2, 10})
    void testHasTagsWithOneUnknownTag(int numTags) {
        generateTags(testEvent, numTags);
        hasTagsExpressionFunction = createObjectUnderTest();
        for (int i = 0; i < numTags; i++) {
            String unknownTag = "\""+RandomStringUtils.randomAlphabetic(5)+"\"";
            List<Object> tagsList = tags.subList(0, i);
            tagsList.add((Object)unknownTag);
            assertThat(hasTagsExpressionFunction.evaluate(tagsList, testEvent, testFunction), equalTo(false));
        }
    }

    @Test
    void testHasTagsWithZeroTags() {
        hasTagsExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> hasTagsExpressionFunction.evaluate(List.of(), testEvent, testFunction));
    }

    @Test
    void testHasTagsWithMissingTag() {
        hasTagsExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> hasTagsExpressionFunction.evaluate(List.of(""), testEvent, testFunction));
    }

    @Test
    void testHasTagsWithEmptyTag() {
        hasTagsExpressionFunction = createObjectUnderTest();
        assertThat(hasTagsExpressionFunction.evaluate(List.of("\"\""), testEvent, testFunction), equalTo(false));
    }

    @Test
    void testHasTagsWithNonStringTags() {
        hasTagsExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> hasTagsExpressionFunction.evaluate(List.of(30), testEvent, testFunction));
    }

    @Test
    void testHasTagsWithStringTagsWithOutQuotes() {
        hasTagsExpressionFunction = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> hasTagsExpressionFunction.evaluate(List.of("tag"), testEvent, testFunction));
    }

}
