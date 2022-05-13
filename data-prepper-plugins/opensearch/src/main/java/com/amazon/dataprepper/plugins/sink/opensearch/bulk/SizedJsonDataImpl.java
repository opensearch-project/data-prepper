/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;

class SizedJsonDataImpl implements SizedJsonData {
    private final JsonData innerJsonData;
    private final long documentSize;

    public SizedJsonDataImpl(final JsonData innerJsonData, final long documentSize) {
        this.innerJsonData = innerJsonData;
        this.documentSize = documentSize;
    }

    @Override
    public long getDocumentSize() {
        return documentSize;
    }

    @Override
    public JsonValue toJson() {
        return innerJsonData.toJson();
    }

    @Override
    public JsonValue toJson(JsonpMapper mapper) {
        return innerJsonData.toJson(mapper);
    }

    @Override
    public <T> T to(Class<T> clazz) {
        return innerJsonData.to(clazz);
    }

    @Override
    public <T> T to(Class<T> clazz, JsonpMapper mapper) {
        return innerJsonData.to(clazz, mapper);
    }

    @Override
    public <T> T deserialize(JsonpDeserializer<T> deserializer) {
        return innerJsonData.deserialize(deserializer);
    }

    @Override
    public <T> T deserialize(JsonpDeserializer<T> deserializer, JsonpMapper mapper) {
        return innerJsonData.deserialize(deserializer, mapper);
    }

    @Override
    public void serialize(JsonGenerator generator, JsonpMapper mapper) {
        innerJsonData.serialize(generator, mapper);
    }
}
