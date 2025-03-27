/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum IndexType {
    TRACE_ANALYTICS_RAW("trace-analytics-raw"),
    TRACE_ANALYTICS_RAW_STANDARD("trace-analytics-standard-raw"),
    TRACE_ANALYTICS_SERVICE_MAP("trace-analytics-service-map"),
    LOG_ANALYTICS("log-analytics"),
    LOG_ANALYTICS_STANDARD("log-analytics-standard"),
    METRIC_ANALYTICS("metric-analytics"),
    METRIC_ANALYTICS_STANDARD("metric-analytics-standard"),
    CUSTOM("custom"),
    MANAGEMENT_DISABLED("management_disabled");

    private final String value;

    /**
     * This is a reverse-lookup map for getting a IndexType from a value.
     */
    private static final Map<String, IndexType> STRING_TO_INDEX_TYPE_MAP = new HashMap<>();

    static {
        Arrays.stream(IndexType.values())
                .forEach(indexType -> STRING_TO_INDEX_TYPE_MAP.put(indexType.value, indexType));
    }

    IndexType(final String value){
        this.value = value;
    }

    public String getValue(){
        return value;
    }

    /**
     * This is for getting an IndexType enum from a given string value.
     * @param value The string value of an IndexType enum
     * @return IndexType enum matching the string value
     */
    static Optional<IndexType> getByValue(final String value) {
        return Optional.ofNullable(STRING_TO_INDEX_TYPE_MAP.get(value));
    }

    /**
     * This flattens all values into a string which can be used for logging or showing what values are supported
     * for the index_type parameter
     * @return a string containing all values that are supported for the index_type parameter
     */
    static String getIndexTypeValues() {
        return Arrays.stream(IndexType.values())
                .map(IndexType::getValue)
                .collect(Collectors.toList())
                .toString();
    }
}
