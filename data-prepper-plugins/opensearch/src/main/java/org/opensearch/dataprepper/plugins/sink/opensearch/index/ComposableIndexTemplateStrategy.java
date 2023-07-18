/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonParser;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.ObjectBuilderDeserializer;
import org.opensearch.client.json.ObjectDeserializer;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.GetIndexTemplateResponse;
import org.opensearch.client.opensearch.indices.PutIndexTemplateRequest;
import org.opensearch.client.opensearch.indices.get_index_template.IndexTemplateItem;
import org.opensearch.client.opensearch.indices.put_index_template.IndexTemplateMapping;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link TemplateStrategy} for the OpenSearch <a href="https://opensearch.org/docs/latest/im-plugin/index-templates/">index template</a>.
 */
class ComposableIndexTemplateStrategy implements TemplateStrategy {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final OpenSearchClient openSearchClient;

    public ComposableIndexTemplateStrategy(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    @Override
    public Optional<Long> getExistingTemplateVersion(final String templateName) throws IOException {
        return getIndexTemplate(templateName)
                .map(IndexTemplateItem::indexTemplate)
                .map(indexTemplate -> indexTemplate.version());
    }

    @Override
    public IndexTemplate createIndexTemplate(final Map<String, Object> templateMap) {
        return new ComposableIndexTemplate(templateMap);
    }

    @Override
    public void createTemplate(final IndexConfiguration indexConfiguration, final IndexTemplate indexTemplate) throws IOException {
        if(!(indexTemplate instanceof ComposableIndexTemplate)) {
            throw new IllegalArgumentException("Unexpected indexTemplate provided to createTemplate.");
        }

        final ComposableIndexTemplate composableIndexTemplate = (ComposableIndexTemplate) indexTemplate;

        final Map<String, Object> templateMapping = composableIndexTemplate.indexTemplateMap;

        final String indexTemplateString = OBJECT_MAPPER.writeValueAsString(templateMapping);

        final ByteArrayInputStream byteIn = new ByteArrayInputStream(
                indexTemplateString.getBytes(StandardCharsets.UTF_8));
        final JsonpMapper mapper = openSearchClient._transport().jsonpMapper();
        final JsonParser parser = mapper.jsonProvider().createParser(byteIn);

        final PutIndexTemplateRequest putIndexTemplateRequest = PutIndexTemplateRequestDeserializer.getJsonpDeserializer(composableIndexTemplate.name)
                .deserialize(parser, mapper);

        openSearchClient.indices().putIndexTemplate(putIndexTemplateRequest);

    }

    private Optional<IndexTemplateItem> getIndexTemplate(final String indexTemplateName) throws IOException {
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
        final GetIndexTemplateResponse indexTemplateResponse = openSearchClient.indices().getIndexTemplate(getRequest);

        final List<IndexTemplateItem> indexTemplateItems = indexTemplateResponse.indexTemplates();
        if (indexTemplateItems.size() == 1) {
            return indexTemplateItems.stream().findFirst();
        } else {
            throw new RuntimeException(String.format("Found zero or multiple index templates result when querying for %s",
                    indexTemplateName));
        }
    }

    static class ComposableIndexTemplate implements IndexTemplate {

        private final Map<String, Object> indexTemplateMap;
        private String name;

        private ComposableIndexTemplate(final Map<String, Object> indexTemplateMap) {
            this.indexTemplateMap = new HashMap<>(indexTemplateMap);
        }

        @Override
        public void setTemplateName(final String name) {
            this.name = name;

        }

        @Override
        public void setIndexPatterns(final List<String> indexPatterns) {
            indexTemplateMap.put("index_patterns", indexPatterns);
        }

        @Override
        public void putCustomSetting(final String name, final Object value) {

        }

        @Override
        public Optional<Long> getVersion() {
            if(!indexTemplateMap.containsKey("version"))
                return Optional.empty();
            final Number version = (Number) indexTemplateMap.get("version");
            return Optional.of(version.longValue());
        }
    }

    private static class PutIndexTemplateRequestDeserializer {
        private static void setupPutIndexTemplateRequestDeserializer(final ObjectDeserializer<PutIndexTemplateRequest.Builder> objectDeserializer) {

            objectDeserializer.add(PutIndexTemplateRequest.Builder::name, JsonpDeserializer.stringDeserializer(), "name");
            objectDeserializer.add(PutIndexTemplateRequest.Builder::indexPatterns, JsonpDeserializer.arrayDeserializer(JsonpDeserializer.stringDeserializer()),
                    "index_patterns");
            objectDeserializer.add(PutIndexTemplateRequest.Builder::version, JsonpDeserializer.longDeserializer(), "version");
            objectDeserializer.add(PutIndexTemplateRequest.Builder::priority, JsonpDeserializer.integerDeserializer(), "priority");
            objectDeserializer.add(PutIndexTemplateRequest.Builder::composedOf, JsonpDeserializer.arrayDeserializer(JsonpDeserializer.stringDeserializer()),
                    "composed_of");
            objectDeserializer.add(PutIndexTemplateRequest.Builder::template, IndexTemplateMapping._DESERIALIZER, "template");
        }

        static JsonpDeserializer<PutIndexTemplateRequest> getJsonpDeserializer(final String name) {
            return ObjectBuilderDeserializer
                    .lazy(
                            () -> new PutIndexTemplateRequest.Builder().name(name),
                            PutIndexTemplateRequestDeserializer::setupPutIndexTemplateRequestDeserializer);
        }
    }
}
