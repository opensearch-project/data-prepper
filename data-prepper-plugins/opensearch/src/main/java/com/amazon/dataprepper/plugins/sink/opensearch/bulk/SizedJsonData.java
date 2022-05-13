/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

import jakarta.json.spi.JsonProvider;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;

import java.io.StringReader;

/**
 * Extends the {@link JsonData} interface from the opensearch-java client with
 * the addition of having a document size.
 */
public interface SizedJsonData extends JsonData {
    /**
     * The size of the document represented by this {@link JsonData}.
     *
     * @return The document size in bytes
     */
    long getDocumentSize();

    /**
     * Creates a new {@link SizedJsonData} from a JSON string.
     *
     * @param jsonString The serialized JSON string which forms this JSON data.
     * @param jsonpMapper The {@link JsonpMapper} to use for mapping.
     * @return A new {@link SizedJsonData}.
     */
    static SizedJsonData fromString(String jsonString, JsonpMapper jsonpMapper) {
        JsonProvider jsonProvider = jsonpMapper.jsonProvider();
        final JsonData jsonData = JsonData.from(jsonProvider.createParser(new StringReader(jsonString)), jsonpMapper);

        final String serializedJsonLength = jsonData.toJson().toString();

        return new SizedJsonDataImpl(jsonData, serializedJsonLength.length());
    }
}
