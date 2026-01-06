/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect.Fields;

public class AppendField extends Field implements Comparable<AppendField> {
    private int index;
    public AppendField(String key) {
        this.setKey(key);
    }

    public void setIndex(int index){
        this.index = index;
    }

    public int getIndex(){
        return index;
    }


    @Override
    public int compareTo(AppendField appendField) {
        return this.index - appendField.getIndex();
    }
}
