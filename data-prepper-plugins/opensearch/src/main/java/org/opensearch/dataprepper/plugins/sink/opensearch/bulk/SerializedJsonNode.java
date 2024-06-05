/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.util.Optional;

class SerializedJsonNode implements SerializedJson, Serializable {
    private byte[] document;
    private JsonNode jsonNode;
    private String documentId = null;
    private String routingField = null;
    private String pipelineField = null;

    public SerializedJsonNode(final JsonNode jsonNode, SerializedJson doc) {
        this.jsonNode = jsonNode;
        this.documentId = doc.getDocumentId().orElse(null);
        this.routingField = doc.getRoutingField().orElse(null);
        this.document = jsonNode.toString().getBytes();
        this.pipelineField = doc.getPipelineField().orElse(null);;
    }

    public SerializedJsonNode(final JsonNode jsonNode) {
        this.jsonNode = jsonNode;
        this.document = jsonNode.toString().getBytes();
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
