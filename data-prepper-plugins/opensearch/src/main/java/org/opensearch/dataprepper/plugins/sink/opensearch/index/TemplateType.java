/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.OpenSearchClient;

import java.util.function.Function;

/**
 * Represents a template type in OpenSearch.
 */
public enum TemplateType {
    /**
     * The legacy template type.
     */
    LEGACY("legacy", LegacyTemplateStrategy::new);

    private final String name;
    private final Function<OpenSearchClient, TemplateStrategy> factoryFunction;

    TemplateType(final String name, final Function<OpenSearchClient, TemplateStrategy> factoryFunction) {
        this.name = name;
        this.factoryFunction = factoryFunction;
    }

    public TemplateStrategy createTemplateStrategy(final OpenSearchClient openSearchClient) {
        return factoryFunction.apply(openSearchClient);
    }
}
