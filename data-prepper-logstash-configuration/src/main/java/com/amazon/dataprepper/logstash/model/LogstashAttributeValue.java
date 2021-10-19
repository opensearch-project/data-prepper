package com.amazon.dataprepper.logstash.model;

/**
 * Class to hold Logstash configuration attribute value and {@link LogstashValueType}
 *
 * @since 1.2
 */
public class LogstashAttributeValue {
    private LogstashValueType attributeValueType;
    private Object value;

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
