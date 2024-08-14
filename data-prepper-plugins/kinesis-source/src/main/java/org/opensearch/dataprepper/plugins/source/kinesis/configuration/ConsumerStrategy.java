package org.opensearch.dataprepper.plugins.source.kinesis.configuration;

import com.fasterxml.jackson.annotation.JsonValue;

// Reference: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html

public enum ConsumerStrategy {

    POLLING("Polling"),

    ENHANCED_FAN_OUT("Fan-Out");

    private final String value;

    ConsumerStrategy(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
