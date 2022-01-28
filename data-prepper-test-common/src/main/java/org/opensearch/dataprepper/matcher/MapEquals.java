/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.matcher;/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapEquals extends TypeSafeMatcher<Map<String, Object>> {
    private Map<String, Object> expectedMap;

    public MapEquals(Map<String, Object> map) {
        expectedMap = map;
    }

    @Override
    protected boolean matchesSafely(Map<String, Object> actualMap) {
        Set<String> keys = new HashSet<>();
        keys.addAll(actualMap.keySet());
        keys.addAll(expectedMap.keySet());

        for (String key : keys) {
            Object actualValue = actualMap.get(key);
            Object expectedValue = expectedMap.get(key);
            if ((!key.equals("@timestamp")) && (actualValue == null || expectedValue == null || !actualValue.equals(expectedValue)))
                return false;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(String.valueOf(expectedMap));
    }

    public static MapEquals isEqualWithoutTimestamp(Map<String, Object> expectedMap) {
        return new MapEquals(expectedMap);
    }
}
