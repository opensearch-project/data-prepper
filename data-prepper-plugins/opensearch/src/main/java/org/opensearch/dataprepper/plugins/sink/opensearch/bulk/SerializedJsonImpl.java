/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import java.io.Serializable;
import java.util.Optional;

class SerializedJsonImpl implements SerializedJson, Serializable {
    private byte[] document;
    private String documentId = null;
    private String routingField = null;
    private String pipelineField = null;

    public SerializedJsonImpl(final byte[] document, String docId, String routingField, String pipelineField) {
        this.document = document;
        this.documentId = docId;
        this.routingField = routingField;
        this.pipelineField = pipelineField;
    }

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

    @Override
    public Optional<String> getDocumentId() {
        return Optional.ofNullable(documentId);
    }

    @Override
    public Optional<String> getRoutingField() {
        return Optional.ofNullable(routingField);
    }

    @Override
    public Optional<String> getPipelineField() { return Optional.ofNullable(pipelineField); }
}
