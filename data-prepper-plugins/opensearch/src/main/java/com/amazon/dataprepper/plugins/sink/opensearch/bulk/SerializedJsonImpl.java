/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

class SerializedJsonImpl implements SerializedJson {
    private byte[] document;

    public SerializedJsonImpl(final byte[] document) {
        this.document = document;
    }

    @Override
    public long getDocumentSize() {
        return document.length;
    }

    @Override
    public byte[] getSerializedJson() {
        return document;
    }
}
