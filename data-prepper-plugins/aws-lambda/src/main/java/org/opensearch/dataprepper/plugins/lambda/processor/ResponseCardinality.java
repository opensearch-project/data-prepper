package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.HashMap;
import java.util.Map;

public enum ResponseCardinality {
    STRICT("strict"),
    AGGREGATE("aggregate");

    private final String value;

    ResponseCardinality(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    private static final Map<String, ResponseCardinality> RESPONSE_CARDINALITY_MAP = new HashMap<>();

    static {
        for (ResponseCardinality type : ResponseCardinality.values()) {
            RESPONSE_CARDINALITY_MAP.put(type.getValue(), type);
        }
    }

    @JsonCreator
    public static ResponseCardinality fromString(String value) {
        return RESPONSE_CARDINALITY_MAP.get(value);
    }
}

