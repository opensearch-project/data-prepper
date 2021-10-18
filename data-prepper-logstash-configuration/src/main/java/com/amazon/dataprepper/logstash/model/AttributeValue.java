package com.amazon.dataprepper.logstash.model;

public class AttributeValue {
    ValueType attributeValueType;
    Object value;

    public ValueType getAttributeValueType() {
        return attributeValueType;
    }

    public void setAttributeValueType(ValueType attributeValueType) {
        this.attributeValueType = attributeValueType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
