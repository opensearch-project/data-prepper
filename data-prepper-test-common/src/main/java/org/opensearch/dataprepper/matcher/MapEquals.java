/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.matcher;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Custom Hamcrest assertion {@link MapEquals#isEqualWithoutTimestamp(Map)} which compares two maps
 * by ignoring {@link MapEquals#DEFAULT_TIMESTAMP_KEY_FOR_EVENT} key from maps.
 * @since 1.3
 */
public class MapEquals extends TypeSafeMatcher<Map<String, Object>> {
    private final Map<String, Object> expectedMap;
    private final String DEFAULT_TIMESTAMP_KEY_FOR_EVENT = "@timestamp";

    MapEquals(Map<String, Object> map) {
        expectedMap = map;
    }

    @Override
    protected boolean matchesSafely(final Map<String, Object> actualMap) {
        Set<String> keys = new HashSet<>();
        keys.addAll(actualMap.keySet());
        keys.addAll(expectedMap.keySet());

        for (String key : keys) {
            Object actualValue = actualMap.get(key);
            Object expectedValue = expectedMap.get(key);
            if ((!key.equals(DEFAULT_TIMESTAMP_KEY_FOR_EVENT)) && (actualValue == null || expectedValue == null || !actualValue.equals(expectedValue)))
                return false;
        }
        return true;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText(String.valueOf(expectedMap));
    }

    /**
     * @return {@link MapEquals}
     */
    public static MapEquals isEqualWithoutTimestamp(final Map<String, Object> expectedMap) {
        return new MapEquals(expectedMap);
    }
}
