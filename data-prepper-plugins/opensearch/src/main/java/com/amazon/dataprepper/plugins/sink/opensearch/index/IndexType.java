package com.amazon.dataprepper.plugins.sink.opensearch.index;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum IndexType {
    TRACE_ANALYTICS_RAW("trace-analytics-raw"),
    TRACE_ANALYTICS_SERVICE_MAP("trace-analytics-service-map"),
    CUSTOM("custom");

    private final String valueString;

    // Reverse-lookup map for getting a IndexType from a name
    private static final Map<String, IndexType> STRING_TO_INDEX_TYPE_MAP = new HashMap<>();

    static {
        Arrays.stream(IndexType.values())
                .forEach(indexType -> STRING_TO_INDEX_TYPE_MAP.put(indexType.valueString, indexType));
    }

    IndexType(final String name){
        this.valueString = name;
    }

    public String getValueString(){
        return valueString;
    }

    public static Optional<IndexType> get(final String value) {
        return Optional.ofNullable(STRING_TO_INDEX_TYPE_MAP.get(value));
    }

    public static List<String> getIndexTypeValueStrings() {
        return Arrays.stream(IndexType.values())
                .map(IndexType::getValueString)
                .collect(Collectors.toList());
    }
}
