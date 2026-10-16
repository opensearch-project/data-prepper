/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.cluster.ExistsComponentTemplateRequest;
import org.opensearch.client.opensearch.cluster.PutComponentTemplateResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Client wrapper for the OpenSearch component template APIs.
 */
class ComponentTemplateAPIWrapper {
    private final OpenSearchClient openSearchClient;

    ComponentTemplateAPIWrapper(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    /**
     * Gets the version of an existing component template.
     *
     * @param name The component template name
     * @return An empty {@link Optional} if the component template does not exist, otherwise its version,
     *         or 0 if it has none.
     * @throws IOException An exception reading from OpenSearch.
     */
    Optional<Long> getComponentTemplateVersion(final String name) throws IOException {
        final ExistsComponentTemplateRequest existsRequest = new ExistsComponentTemplateRequest.Builder()
                .name(name)
                .build();
        if (!openSearchClient.cluster().existsComponentTemplate(existsRequest).value()) {
            return Optional.empty();
        }

        // Read the raw response: the typed GetComponentTemplateResponse requires template.settings,
        // which OpenSearch omits for a component template with mappings only.
        final JsonNode componentTemplates = openSearchClient._transport().performRequest(
                        null, createGetEndpoint(name), openSearchClient._transportOptions())
                .to(JsonNode.class)
                .path("component_templates");
        if (componentTemplates.size() != 1) {
            throw new RuntimeException(String.format("Found zero or multiple component templates result when querying for %s", name));
        }
        return Optional.of(componentTemplates.get(0).path("component_template").path("version").asLong(0L));
    }

    /**
     * Creates or replaces a component template.
     *
     * @param name The component template name
     * @param componentTemplate The request body, with {@code template} and optionally {@code version}
     * @throws IOException An exception writing to OpenSearch.
     */
    void putComponentTemplate(final String name, final Map<String, Object> componentTemplate) throws IOException {
        openSearchClient._transport().performRequest(
                componentTemplate,
                createEndpoint(name),
                openSearchClient._transportOptions());
    }

    private static Endpoint<Void, JsonData, ErrorResponse> createGetEndpoint(final String name) {
        final String path = "/_component_template/" + name;

        return new SimpleEndpoint<>(
                request -> "GET",
                request -> path,
                request -> Collections.emptyMap(),
                SimpleEndpoint.emptyMap(),
                false,
                JsonData._DESERIALIZER);
    }

    private Endpoint<Map<String, Object>, PutComponentTemplateResponse, ErrorResponse> createEndpoint(final String name) {
        final String path = "/_component_template/" + name;

        return new SimpleEndpoint<>(
                request -> "PUT",
                request -> path,
                request -> Collections.emptyMap(),
                SimpleEndpoint.emptyMap(),
                true,
                PutComponentTemplateResponse._DESERIALIZER);
    }
}
