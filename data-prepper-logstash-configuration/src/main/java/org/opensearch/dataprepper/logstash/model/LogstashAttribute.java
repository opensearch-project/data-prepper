package org.opensearch.dataprepper.logstash.model;

import static com.google.common.base.Preconditions.checkNotNull;

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

    private LogstashAttribute (Builder builder) {
        checkNotNull(builder.attributeName, "attribute name cannot be null");
        checkNotNull(builder.attributeValue, "attribute value cannot be null");

        this.attributeName = builder.attributeName;
        this.attributeValue = builder.attributeValue;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A Builder for creating {@link LogstashAttribute} instances.
     *
     * @since 1.2
     */
    public static class Builder {
        private String attributeName;
        private LogstashAttributeValue attributeValue;

        public Builder attributeName(final String attributeName) {
            this.attributeName = attributeName;
            return this;
        }

        public Builder attributeValue(final LogstashAttributeValue attributeValue) {
            this.attributeValue = attributeValue;
            return this;
        }

        public LogstashAttribute build() {
            return new LogstashAttribute(this);
        }
    }
}
