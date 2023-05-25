/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.model;

import jakarta.json.stream.JsonGenerator;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import org.opensearch.dataprepper.plugins.source.opensearch.codec.JacksonValueParser;

import java.util.HashMap;
import java.util.Map;

public class PitRequest implements JsonpSerializable {

    private StringBuilder index;

    private String keepAlive;

    private static final String POST_REQUEST = "POST";

    private static  final String PIT_ID_URL =  "/_search/point_in_time";

    public PitRequest(PitBuilder builder) {
        this.index = builder.index;
        this.keepAlive = builder.keepAlive;
    }

    public Map<String,String> queryParameters = new HashMap<>();

    public static final JsonpDeserializer<Map> deserializer = new JacksonValueParser<>(Map.class);

    public void setQueryParameters(Map<String, String> queryParameters) {
        this.queryParameters = queryParameters;
    }

    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }

    private Map<String,String> params = new HashMap<>();

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public static final Endpoint<PitRequest, Map, ErrorResponse> ENDPOINT =
            new SimpleEndpoint<>(
                    r -> POST_REQUEST,
                    r -> "http://localhost:9200/"+r.getIndex() + PIT_ID_URL,
                    r-> r.getQueryParameters(),
                    SimpleEndpoint.emptyMap(),
                    true,
                    deserializer
            );

    public StringBuilder getIndex() {
        return index;
    }

    public void setIndex(StringBuilder index) {
        this.index = index;
    }

    public void setKeepAlive(String keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public void serialize(JsonGenerator generator, JsonpMapper mapper) {
        generator.writeStartObject();
    }
}