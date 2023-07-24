package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonParser;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;
import org.opensearch.client.opensearch.indices.TemplateMapping;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public class OpenSearchLegacyTemplateAPIWrapper implements IndexTemplateAPIWrapper<TemplateMapping> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final OpenSearchClient openSearchClient;

    public OpenSearchLegacyTemplateAPIWrapper(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    @Override
    public void putTemplate(IndexTemplate indexTemplate) throws IOException {
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
        openSearchIndicesClient.putTemplate(putTemplateRequest);
    }

    @Override
    public Optional<TemplateMapping> getTemplate(final String templateName) throws IOException {
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
        final GetTemplateResponse response = openSearchClient.indices().getTemplate(getTemplateRequest);

        if (response.result().size() == 1) {
            return response.result().values().stream().findFirst();
        } else {
            throw new RuntimeException(String.format("Found zero or multiple index templates result when querying for %s",
                    templateName));
        }
    }
}
