/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect.Fields;

public abstract class Field {
    boolean stripTrailing = false;
    private String key;
    private String value;
    private Field next;

    public String getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public Field getNext() {
        return next;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setNext(Field next) {
        this.next = next;
    }

    public void setValue(String value) {
        this.value = stripTrailing ? value.stripTrailing() : value;
    }

}
