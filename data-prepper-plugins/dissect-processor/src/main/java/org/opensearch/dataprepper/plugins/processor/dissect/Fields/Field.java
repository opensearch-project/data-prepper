/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect.Fields;

public abstract class Field {
    boolean stripTrailing = false;
    private String key;
    private Field next;

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

    public boolean isStripTrailing() {
        return stripTrailing;
    }

}
