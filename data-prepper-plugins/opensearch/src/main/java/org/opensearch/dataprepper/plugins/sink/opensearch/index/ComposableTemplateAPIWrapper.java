package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateResponse;
import org.opensearch.client.opensearch.indices.PutIndexTemplateResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class ComposableTemplateAPIWrapper implements IndexTemplateAPIWrapper<GetIndexTemplateResponse> {
    private final OpenSearchClient openSearchClient;

    public ComposableTemplateAPIWrapper(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    @Override
    public void putTemplate(final IndexTemplate indexTemplate) throws IOException {
        if(!(indexTemplate instanceof ComposableIndexTemplate)) {
            throw new IllegalArgumentException("Unexpected indexTemplate provided to createTemplate.");
        }

        final ComposableIndexTemplate composableIndexTemplate = (ComposableIndexTemplate) indexTemplate;
        Map<String, Object> indexTemplateMap = composableIndexTemplate.getIndexTemplateMap();

        openSearchClient._transport().performRequest(
                indexTemplateMap,
                createEndpoint(composableIndexTemplate),
                openSearchClient._transportOptions());
    }

    @Override
    public Optional<GetIndexTemplateResponse> getTemplate(final String indexTemplateName) throws IOException {
        final ExistsIndexTemplateRequest existsRequest = new ExistsIndexTemplateRequest.Builder()
                .name(indexTemplateName)
                .build();
        final BooleanResponse existsResponse = openSearchClient.indices().existsIndexTemplate(existsRequest);

        if (!existsResponse.value()) {
            return Optional.empty();
        }

        final GetIndexTemplateRequest getRequest = new GetIndexTemplateRequest.Builder()
                .name(indexTemplateName)
                .build();
        return Optional.of(openSearchClient.indices().getIndexTemplate(getRequest));
    }

    private Endpoint<Map<String, Object>, PutIndexTemplateResponse, ErrorResponse> createEndpoint(final ComposableIndexTemplate composableIndexTemplate) {
        final String path = "/_index_template/" + composableIndexTemplate.getName();

        return new SimpleEndpoint<>(
                request -> "PUT",
                request -> path,
                request -> Collections.emptyMap(),
                SimpleEndpoint.emptyMap(),
                true,
                PutIndexTemplateResponse._DESERIALIZER);
    }
}
