package com.amazon.dataprepper.logstash.model;

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

    private LogstashAttributeValue (LogstashAttributeValueBuilder builder) {
        this.attributeValueType = builder.attributeValueType;
        this.value = builder.value;
    }

    public static class LogstashAttributeValueBuilder {
        private LogstashValueType attributeValueType;
        private Object value;

        public LogstashAttributeValueBuilder attributeValueType(LogstashValueType attributeValueType) {
            this.attributeValueType = attributeValueType;
            return this;
        }

        public LogstashAttributeValueBuilder value(Object value) {
            this.value = value;
            return this;
        }

        public LogstashAttributeValue build() {
            return new LogstashAttributeValue(this);
        }
    }
}
