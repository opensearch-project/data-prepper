package org.opensearch.dataprepper.logstash.model;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class to hold Logstash configuration attribute value and {@link LogstashValueType}
 *
 * @since 1.2
 */
public class LogstashAttributeValue {
    private final LogstashValueType attributeValueType;
    private final Object value;

    public LogstashValueType getAttributeValueType() {
        return attributeValueType;
    }

    public Object getValue() {
        return value;
    }

    private LogstashAttributeValue (Builder builder) {
        checkNotNull(builder.attributeValueType, "attribute value type cannot be null");
        checkNotNull(builder.value, "attribute value cannot be null");

        this.attributeValueType = builder.attributeValueType;
        this.value = builder.value;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A Builder for creating {@link LogstashAttributeValue} instances.
     *
     * @since 1.2
     */
    public static class Builder {
        private LogstashValueType attributeValueType;
        private Object value;

        public Builder attributeValueType(final LogstashValueType attributeValueType) {
            this.attributeValueType = attributeValueType;
            return this;
        }

        public Builder value(final Object value) {
            this.value = value;
            return this;
        }

        public LogstashAttributeValue build() {
            return new LogstashAttributeValue(this);
        }
    }
}
