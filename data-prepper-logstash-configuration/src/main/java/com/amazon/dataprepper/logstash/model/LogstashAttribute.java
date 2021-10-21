package com.amazon.dataprepper.logstash.model;

/**
 * Class to hold Logstash configuration attribute name and {@link LogstashAttributeValue}
 *
 * @since 1.2
 */
public class LogstashAttribute {
    private final String attributeName;
    private final LogstashAttributeValue attributeValue;

    public String getAttributeName() {
        return attributeName;
    }

    public LogstashAttributeValue getAttributeValue() {
        return attributeValue;
    }

    private LogstashAttribute (LogstashAttributeBuilder builder) {
        this.attributeName = builder.attributeName;
        this.attributeValue = builder.attributeValue;
    }

    public static class LogstashAttributeBuilder {
        private String attributeName;
        private LogstashAttributeValue attributeValue;

        public LogstashAttributeBuilder attributeName(final String attributeName) {
            this.attributeName = attributeName;
            return this;
        }

        public LogstashAttributeBuilder attributeValue(final LogstashAttributeValue attributeValue) {
            this.attributeValue = attributeValue;
            return this;
        }

        public LogstashAttribute build() {
            return new LogstashAttribute(this);
        }
    }
}
