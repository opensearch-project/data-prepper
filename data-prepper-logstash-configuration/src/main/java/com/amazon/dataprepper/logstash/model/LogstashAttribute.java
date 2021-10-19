package com.amazon.dataprepper.logstash.model;

public class LogstashAttribute {
    String attributeName;
    LogstashAttributeValue attributeValue;

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
