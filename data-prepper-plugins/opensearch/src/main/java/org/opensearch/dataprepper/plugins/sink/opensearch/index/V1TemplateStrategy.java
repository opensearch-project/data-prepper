/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.indices.GetTemplateResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

class V1TemplateStrategy implements TemplateStrategy {
    private final IndexTemplateAPIWrapper<GetTemplateResponse> indexTemplateAPIWrapper;

    public V1TemplateStrategy(final IndexTemplateAPIWrapper<GetTemplateResponse> indexTemplateAPIWrapper) {
        this.indexTemplateAPIWrapper = indexTemplateAPIWrapper;
    }

    @Override
    public Optional<Long> getExistingTemplateVersion(final String templateName) throws IOException {
        return indexTemplateAPIWrapper.getTemplate(templateName)
                .map(response -> {
                    if (response.result().size() == 1) {
                        return response.result().values().stream().findFirst().get().version();
                    } else {
                        throw new RuntimeException(String.format("Found zero or multiple index templates result when querying for %s",
                                templateName));
                    }
                });
    }

    @Override
    public IndexTemplate createIndexTemplate(final Map<String, Object> templateMap) {
        return new LegacyIndexTemplate(templateMap);
    }

    @Override
    public void createTemplate(final IndexTemplate indexTemplate) throws IOException {
        indexTemplateAPIWrapper.putTemplate(indexTemplate);
    }
}
