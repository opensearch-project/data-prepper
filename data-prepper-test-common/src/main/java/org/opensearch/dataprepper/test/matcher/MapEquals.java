/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.matcher;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom Hamcrest assertion {@link MapEquals#isEqualWithoutTimestamp(Map)} which compares two maps
 * by ignoring {@link MapEquals#DEFAULT_TIMESTAMP_KEY_FOR_EVENT} key from maps.
 * @since 1.3
 */
public class MapEquals extends TypeSafeMatcher<Map<String, Object>> {
    private final Map<String, Object> expectedMap;
    static final String DEFAULT_TIMESTAMP_KEY_FOR_EVENT = "@timestamp";

    MapEquals(Map<String, Object> map) {
        expectedMap = map;
    }

    @Override
    protected boolean matchesSafely(final Map<String, Object> actualMap) {
        Map<String, Object> modifiedExpectedMap = new HashMap<>(expectedMap);
        Map<String, Object> modifiedActualMap = new HashMap<>(actualMap);

        modifiedExpectedMap.remove(DEFAULT_TIMESTAMP_KEY_FOR_EVENT);
        modifiedActualMap.remove(DEFAULT_TIMESTAMP_KEY_FOR_EVENT);

        return modifiedExpectedMap.equals(modifiedActualMap);
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText(String.valueOf(expectedMap));
    }

    /**
     * Custom matcher which matches map by ignoring default timestamp
     * @param expectedMap a Map for comparison
     * @return {@link MapEquals}
     * @since 1.3
     */
    public static MapEquals isEqualWithoutTimestamp(final Map<String, Object> expectedMap) {
        return new MapEquals(expectedMap);
    }
}
