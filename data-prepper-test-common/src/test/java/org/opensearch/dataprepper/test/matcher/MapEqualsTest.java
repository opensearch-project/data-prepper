/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.matcher;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.matcher.MapEquals.isEqualWithoutTimestamp;

class MapEqualsTest {
    private static final Map<String, Object> expectedMap1 = new HashMap<>();
    private static final Map<String, Object> expectedMap2 = new HashMap<>();

    @BeforeAll
    static void setup() {
        expectedMap1.put("List", Arrays.asList(1, 2, 3));
        expectedMap1.put(MapEquals.DEFAULT_TIMESTAMP_KEY_FOR_EVENT, OffsetDateTime.now());

        expectedMap2.put("List", Arrays.asList(1, 2, 3));
        expectedMap2.put(MapEquals.DEFAULT_TIMESTAMP_KEY_FOR_EVENT, OffsetDateTime.now());
        expectedMap2.put(null, Arrays.asList(1, 2, 3));
    }

    @Test
    void matchesSafely_will_return_true_if_maps_are_equal_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("List", Arrays.asList(1, 2, 3));

        assertThat(actualMap, isEqualWithoutTimestamp(expectedMap1));
    }

    @Test
    void matchesSafely_will_return_false_if_maps_are_not_equal_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("List", Arrays.asList(1, 2));

        assertThat(actualMap, not(isEqualWithoutTimestamp(expectedMap1)));
    }

    @Test
    void matchesSafely_will_return_true_if_maps_with_timestamp_are_not_equal_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("List", Arrays.asList(1, 2, 3));
        actualMap.put(MapEquals.DEFAULT_TIMESTAMP_KEY_FOR_EVENT, OffsetDateTime.now());

        assertThat(actualMap, isEqualWithoutTimestamp(expectedMap1));
    }

    @Test
    void matchesSafely_will_return_false_if_key_is_missing_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put("anotherList", Arrays.asList(1, 2, 3));
        actualMap.put(MapEquals.DEFAULT_TIMESTAMP_KEY_FOR_EVENT, OffsetDateTime.now());

        assertThat(actualMap, not(isEqualWithoutTimestamp(expectedMap1)));
    }

    @Test
    void matchesSafely_will_return_false_if_one_map_has_null_value_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put(null, null);
        actualMap.put("List", Arrays.asList(1, 2, 3));
        actualMap.put(MapEquals.DEFAULT_TIMESTAMP_KEY_FOR_EVENT, OffsetDateTime.now());

        assertThat(actualMap, not(isEqualWithoutTimestamp(expectedMap2)));
    }

    @Test
    void matchesSafely_will_return_true_if_both_maps_have_null_kaye_Test() {
        Map<String, Object> actualMap = new HashMap<>();
        actualMap.put(null, Arrays.asList(1, 2, 3));
        actualMap.put("List", Arrays.asList(1, 2, 3));
        actualMap.put(MapEquals.DEFAULT_TIMESTAMP_KEY_FOR_EVENT, OffsetDateTime.now());

        assertThat(actualMap, isEqualWithoutTimestamp(expectedMap2));
    }
}