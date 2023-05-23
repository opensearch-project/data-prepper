/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonParser;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;
import org.opensearch.client.opensearch.indices.TemplateMapping;
import org.opensearch.client.transport.endpoints.BooleanResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class V1TemplateStrategy implements TemplateStrategy {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final OpenSearchClient openSearchClient;

    public V1TemplateStrategy(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    @Override
    public Optional<Long> getExistingTemplateVersion(final String templateName) throws IOException {
        return getTemplateMapping(templateName)
                .map(TemplateMapping::version);
    }

    @Override
    public IndexTemplate createIndexTemplate(final Map<String, Object> templateMap) {
        return new LegacyIndexTemplate(templateMap);
    }

    @Override
    public void createTemplate(final IndexTemplate indexTemplate) throws IOException {
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

        openSearchClient.indices().putTemplate(putTemplateRequest);
    }

    private Optional<TemplateMapping> getTemplateMapping(final String templateName) throws IOException {
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

    static class LegacyIndexTemplate implements IndexTemplate {

        public static final String SETTINGS_KEY = "settings";
        private final Map<String, Object> templateMap;

        private LegacyIndexTemplate(final Map<String, Object> templateMap) {
            this.templateMap = new HashMap<>(templateMap);
            if(this.templateMap.containsKey(SETTINGS_KEY)) {
                final HashMap<String, Object> copiedSettings = new HashMap<>((Map<String, Object>) this.templateMap.get(SETTINGS_KEY));
                this.templateMap.put(SETTINGS_KEY, copiedSettings);
            }
        }

        @Override
        public void setTemplateName(final String name) {
            templateMap.put("name", name);
        }

        @Override
        public void setIndexPatterns(final List<String> indexPatterns) {
            templateMap.put("index_patterns", indexPatterns);
        }

        @Override
        public void putCustomSetting(final String name, final Object value) {
            Map<String, Object> settings = (Map<String, Object>) this.templateMap.computeIfAbsent(SETTINGS_KEY, x -> new HashMap<>());
            settings.put(name, value);
        }

        @Override
        public Optional<Long> getVersion() {
            if(!templateMap.containsKey("version"))
                return Optional.empty();
            final Number version = (Number) templateMap.get("version");
            return Optional.of(version.longValue());
        }

        Map<String, Object> getTemplateMap() {
            return this.templateMap;
        }
    }
}
