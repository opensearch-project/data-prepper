package org.opensearch.dataprepper.plugins.lambda.processor;

import java.util.HashMap;
import java.util.Map;

public enum ResponseCardinality {
    STRICT("strict"),
    AGGREGATE("aggregate");

    private final String value;

    ResponseCardinality(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    private static final Map<String, ResponseCardinality> RESPONSE_CARDINALITY_MAP = new HashMap<>();

    static {
        for (ResponseCardinality type : ResponseCardinality.values()) {
            RESPONSE_CARDINALITY_MAP.put(type.getValue().toLowerCase(), type);
        }
    }

    // Default value is STRICT
    public static ResponseCardinality fromString(String value) {
        if (value == null) {
            return STRICT;
        }
        return RESPONSE_CARDINALITY_MAP.getOrDefault(value.toLowerCase(), STRICT);
    }
}

