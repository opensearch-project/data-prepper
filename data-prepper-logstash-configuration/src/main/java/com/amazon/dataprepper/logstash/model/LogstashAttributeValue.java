package com.amazon.dataprepper.logstash.model;

public class LogstashAttributeValue {
    LogstashValueType attributeValueType;
    Object value;

    public LogstashValueType getAttributeValueType() {
        return attributeValueType;
    }

    public void setAttributeValueType(LogstashValueType attributeValueType) {
        this.attributeValueType = attributeValueType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
