/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Unified entity representing a connection between two nodes with optional operation detail.
 * Replaces both ServiceConnection and ServiceOperationDetail.
 *
 * When operations are present, both nodeConnectionHash and operationConnectionHash are populated.
 * When only node-level info is available, only nodeConnectionHash is populated.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NodeOperationDetail {

    @JsonProperty("sourceNode")
    private final Node sourceNode;

    @JsonProperty("targetNode")
    private final Node targetNode;

    @JsonProperty("sourceOperation")
    private final Operation sourceOperation;

    @JsonProperty("targetOperation")
    private final Operation targetOperation;

    @JsonProperty("nodeConnectionHash")
    private final String nodeConnectionHash;

    @JsonProperty("operationConnectionHash")
    private final String operationConnectionHash;

    @JsonProperty("timestamp")
    private final String timestamp;

    public NodeOperationDetail(final Node sourceNode,
                               final Node targetNode,
                               final Operation sourceOperation,
                               final Operation targetOperation,
                               final Instant timestamp) {
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.sourceOperation = sourceOperation;
        this.targetOperation = targetOperation;
        this.timestamp = DateTimeFormatter.ISO_INSTANT.format(timestamp);
        this.nodeConnectionHash = String.valueOf(Objects.hash(sourceNode, targetNode));

        if (sourceOperation != null && sourceOperation.getName() != null) {
            final String targetOpName = targetOperation != null ? targetOperation.getName() : null;
            this.operationConnectionHash = String.valueOf(
                    Objects.hash(sourceNode, targetNode, sourceOperation.getName(), targetOpName));
        } else {
            this.operationConnectionHash = null;
        }
    }

    public Node getSourceNode() {
        return sourceNode;
    }

    public Node getTargetNode() {
        return targetNode;
    }

    public Operation getSourceOperation() {
        return sourceOperation;
    }

    public Operation getTargetOperation() {
        return targetOperation;
    }

    public String getNodeConnectionHash() {
        return nodeConnectionHash;
    }

    public String getOperationConnectionHash() {
        return operationConnectionHash;
    }

    public String getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        NodeOperationDetail that = (NodeOperationDetail) o;
        return Objects.equals(sourceNode, that.sourceNode) &&
                Objects.equals(targetNode, that.targetNode) &&
                Objects.equals(sourceOperation, that.sourceOperation) &&
                Objects.equals(targetOperation, that.targetOperation) &&
                Objects.equals(nodeConnectionHash, that.nodeConnectionHash) &&
                Objects.equals(operationConnectionHash, that.operationConnectionHash) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, targetNode, sourceOperation, targetOperation,
                nodeConnectionHash, operationConnectionHash, timestamp);
    }

    @Override
    public String toString() {
        return "NodeOperationDetail{" +
                "sourceNode=" + sourceNode +
                ", targetNode=" + targetNode +
                ", sourceOperation=" + sourceOperation +
                ", targetOperation=" + targetOperation +
                ", nodeConnectionHash='" + nodeConnectionHash + '\'' +
                ", operationConnectionHash='" + operationConnectionHash + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
