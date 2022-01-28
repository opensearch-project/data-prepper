/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.matcher;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class MapEqualsTest {
    private final Map<String, Object> expectedMap = new HashMap<>();

    private MapEquals createObjectUnderTest() {
        expectedMap.put("List", Arrays.asList(1, 2, 3));
        expectedMap.put("@timestamp", OffsetDateTime.now());

        return new MapEquals(expectedMap);
    }

    @Test
    void matchesSafely_will_return_true_if_maps_are_equal_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("List", Arrays.asList(1, 2, 3));

        assertThat(createObjectUnderTest().matchesSafely(actualMap), is(true));
    }

    @Test
    void matchesSafely_will_return_false_if_maps_are_not_equal_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("List", Arrays.asList(1, 2));

        assertThat(createObjectUnderTest().matchesSafely(actualMap),is(false));
    }

    @Test
    void matchesSafely_will_return_true_if_maps_with_timestamp_are_not_equal_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("List", Arrays.asList(1, 2, 3));
        actualMap.put("@timestamp", OffsetDateTime.now());

        assertThat(createObjectUnderTest().matchesSafely(actualMap), is(true));
    }

    @Test
    void matchesSafely_will_return_false_if_key_is_missing_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("anotherList", Arrays.asList(1, 2, 3));
        actualMap.put("@timestamp", OffsetDateTime.now());

        assertThat(createObjectUnderTest().matchesSafely(actualMap), is(false));
    }

}