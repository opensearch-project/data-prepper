package org.opensearch.dataprepper.plugins.kinesis.source.configuration;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @see <a href="https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html">Enhanced Consumers</a>
 */

public enum ConsumerStrategy {

    POLLING("polling"),

    ENHANCED_FAN_OUT("fan-out");

    private final String value;

    ConsumerStrategy(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
