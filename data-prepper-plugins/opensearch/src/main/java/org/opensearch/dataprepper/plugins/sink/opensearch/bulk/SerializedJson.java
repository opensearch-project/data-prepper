/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents JSON which is already serialized for use in the {@link PreSerializedJsonpMapper}.
 */
public interface SerializedJson extends SizedDocument {
    byte[] getSerializedJson();
    Optional<String> getDocumentId();
    Optional<String> getRoutingField();

    /**
     * Creates a new {@link SerializedJson} from a JSON string and optional documentId and routingField.
     *
     * @param jsonString The serialized JSON string which forms this JSON data.
     * @param docId Optional documment ID string
     * @param routingField Optional routing field string
     * @return A new {@link SerializedJson}.
     */
    static SerializedJson fromStringAndOptionals(String jsonString, String docId, String routingField) {
        Objects.requireNonNull(jsonString);
        return new SerializedJsonImpl(jsonString.getBytes(StandardCharsets.UTF_8), docId, routingField);
    }

    static SerializedJson fromJsonNode(final JsonNode jsonNode, SerializedJson document) {
        return new SerializedJsonNode(jsonNode, document);
    }
}

