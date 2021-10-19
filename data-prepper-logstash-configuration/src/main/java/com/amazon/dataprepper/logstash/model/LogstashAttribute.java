package com.amazon.dataprepper.logstash.model;

/**
 * Class to hold Logstash configuration attribute name and {@link LogstashAttributeValue}
 *
 * @since 1.2
 */
public class LogstashAttribute {
    private String attributeName;
    private LogstashAttributeValue attributeValue;

    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    public LogstashAttributeValue getAttributeValue() {
        return attributeValue;
    }

    public void setAttributeValue(LogstashAttributeValue attributeValue) {
        this.attributeValue = attributeValue;
    }
}
