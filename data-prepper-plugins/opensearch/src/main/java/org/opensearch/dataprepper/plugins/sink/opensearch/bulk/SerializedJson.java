/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents JSON which is already serialized for use in the {@link PreSerializedJsonpMapper}.
 */
public interface SerializedJson extends SizedDocument {
    byte[] getSerializedJson();
    Optional<String> getDocumentId();
    Optional<String> getRoutingField();
    Optional<String> getPipelineField();
    Optional<Map<String, Object>> getResolvedScriptParameters();

    /**
     * Creates a new {@link SerializedJson} from a JSON string and optional documentId and routingField.
     *
     * @param jsonString The serialized JSON string which forms this JSON data.
     * @param docId Optional documment ID string
     * @param routingField Optional routing field string
     * @param pipelineField pipeline Field
     * @return A new {@link SerializedJson}.
     */
    static SerializedJson fromStringAndOptionals(String jsonString, String docId, String routingField, String pipelineField) {
        Objects.requireNonNull(jsonString);
        return new SerializedJsonImpl(jsonString.getBytes(StandardCharsets.UTF_8), docId, routingField, pipelineField, null);
    }

    static SerializedJson fromJsonNode(final JsonNode jsonNode, SerializedJson document) {
        return new SerializedJsonNode(jsonNode, document);
    }

    static Builder builder() {
        return new Builder();
    }

    class Builder {
        private String jsonString;
        private String documentId;
        private String routingField;
        private String pipelineField;
        private Map<String, Object> resolvedScriptParameters;

        public Builder withJsonString(final String jsonString) {
            this.jsonString = jsonString;
            return this;
        }

        public Builder withDocumentId(final String documentId) {
            this.documentId = documentId;
            return this;
        }

        public Builder withRoutingField(final String routingField) {
            this.routingField = routingField;
            return this;
        }

        public Builder withPipelineField(final String pipelineField) {
            this.pipelineField = pipelineField;
            return this;
        }

        public Builder withResolvedScriptParameters(final Map<String, Object> resolvedScriptParameters) {
            this.resolvedScriptParameters = resolvedScriptParameters;
            return this;
        }

        public SerializedJson build() {
            Objects.requireNonNull(jsonString);
            return new SerializedJsonImpl(
                    jsonString.getBytes(StandardCharsets.UTF_8),
                    documentId, routingField, pipelineField, resolvedScriptParameters);
        }
    }
}
