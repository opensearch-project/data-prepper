package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonParser;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;
import org.opensearch.client.opensearch.indices.PutTemplateResponse;
import org.opensearch.client.transport.JsonEndpoint;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Es6IndexTemplateAPIWrapper implements IndexTemplateAPIWrapper<GetTemplateResponse> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final OpenSearchClient openSearchClient;

    public Es6IndexTemplateAPIWrapper(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    @Override
    public void putTemplate(final IndexTemplate indexTemplate) throws IOException {
        if(!(indexTemplate instanceof LegacyIndexTemplate)) {
            throw new IllegalArgumentException("Unexpected indexTemplate provided to createTemplate.");
        }

        final Map<String, Object> templateMapping = ((LegacyIndexTemplate) indexTemplate).getTemplateMap();
        final String indexTemplateString = OBJECT_MAPPER.writeValueAsString(templateMapping);

        // Parse byte array to Map
        final ByteArrayInputStream byteIn = new ByteArrayInputStream(
                indexTemplateString.getBytes(StandardCharsets.UTF_8));
        final JsonpMapper mapper = openSearchClient._transport().jsonpMapper();
        final JsonParser parser = mapper.jsonProvider().createParser(byteIn);

        final PutTemplateRequest putTemplateRequest = PutTemplateRequestDeserializer.getJsonpDeserializer()
                .deserialize(parser, mapper);

        final OpenSearchIndicesClient openSearchIndicesClient = openSearchClient.indices();
        final JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse> endpoint = es6PutTemplateEndpoint(putTemplateRequest);
        openSearchIndicesClient._transport().performRequest(putTemplateRequest, endpoint, openSearchIndicesClient._transportOptions());
    }

    @Override
    public Optional<GetTemplateResponse> getTemplate(final String templateName) throws IOException {
        final ExistsTemplateRequest existsTemplateRequest = new ExistsTemplateRequest.Builder()
                .name(templateName)
                .build();
        final BooleanResponse booleanResponse = openSearchClient.indices().existsTemplate(
                existsTemplateRequest);
        if (!booleanResponse.value()) {
            return Optional.empty();
        }

        final GetTemplateRequest getTemplateRequest = new GetTemplateRequest.Builder()
                .name(templateName)
                .build();
        return Optional.of(openSearchClient.indices().getTemplate(getTemplateRequest));
    }

    private JsonEndpoint<PutTemplateRequest, PutTemplateResponse, ErrorResponse> es6PutTemplateEndpoint(
            final PutTemplateRequest putTemplateRequest) {
        return new SimpleEndpoint<>(

                // Request method
                request -> {
                    return "PUT";

                },

                // Request path
                request -> {
                    final int _name = 1 << 0;

                    int propsSet = 0;

                    propsSet |= _name;

                    if (propsSet == (_name)) {
                        StringBuilder buf = new StringBuilder();
                        buf.append("/_template");
                        buf.append("/");
                        SimpleEndpoint.pathEncode(request.name(), buf);
                        buf.append("?include_type_name=false");
                        return buf.toString();
                    }
                    throw SimpleEndpoint.noPathTemplateFound("path");

                },

                // Request parameters
                request -> {
                    Map<String, String> params = new HashMap<>();
                    if (request.masterTimeout() != null) {
                        params.put("master_timeout", request.masterTimeout()._toJsonString());
                    }
                    if (request.clusterManagerTimeout() != null) {
                        params.put("cluster_manager_timeout", request.clusterManagerTimeout()._toJsonString());
                    }
                    if (request.flatSettings() != null) {
                        params.put("flat_settings", String.valueOf(request.flatSettings()));
                    }
                    if (request.create() != null) {
                        params.put("create", String.valueOf(request.create()));
                    }
                    if (request.timeout() != null) {
                        params.put("timeout", request.timeout()._toJsonString());
                    }
                    return params;

                }, SimpleEndpoint.emptyMap(), true, PutTemplateResponse._DESERIALIZER);
    }
}
