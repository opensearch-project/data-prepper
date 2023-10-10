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

    public SerializedJsonNode(final JsonNode jsonNode, SerializedJson doc) {
        this.jsonNode = jsonNode;
        this.documentId = doc.getDocumentId().get();
        this.routingField = doc.getRoutingField().get();
        this.document = jsonNode.toString().getBytes();
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
}
