package com.amazon.dataprepper.plugins.sink.opensearch.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum IndexType {
    TRACE_ANALYTICS_RAW("trace-analytics-raw"),
    TRACE_ANALYTICS_SERVICE_MAP("trace-analytics-service-map"),
    CUSTOM("custom");

    private final String value;

    // Reverse-lookup map for getting a IndexType from a name
    private static final Map<String, IndexType> STRING_TO_INDEX_TYPE_MAP = new HashMap<>();

    static {
        Arrays.stream(IndexType.values())
                .forEach(indexType -> STRING_TO_INDEX_TYPE_MAP.put(indexType.value, indexType));
    }

    IndexType(final String value){
        this.value = value;
    }

    String getValue(){
        return value;
    }

    static Optional<IndexType> getByValue(final String value) {
        return Optional.ofNullable(STRING_TO_INDEX_TYPE_MAP.get(value));
    }

    static String getIndexTypeValues() {
        return Arrays.toString(IndexType.values());
    }
}
