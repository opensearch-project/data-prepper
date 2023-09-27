/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

public class Delimiter {
    private final String delimiterString;
    private int start = -1;
    private int end = -1;
    private Delimiter next = null;

    private Delimiter prev = null;

    public Delimiter(String delimiterString) {
        this.delimiterString = delimiterString;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int ind) {
        start = ind;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int ind) {
        end = ind;
    }

    public Delimiter getNext() {
        return next;
    }

    public void setNext(Delimiter nextDelimiter) {
        next = nextDelimiter;
    }

    public Delimiter getPrev() {
        return prev;
    }

    public void setPrev(Delimiter prevDelimiter) {
        prev = prevDelimiter;
    }

    @Override
    public String toString() {
        return delimiterString;
    }
}
