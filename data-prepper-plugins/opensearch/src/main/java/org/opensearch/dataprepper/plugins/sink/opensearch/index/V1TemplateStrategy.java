/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.indices.TemplateMapping;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

class V1TemplateStrategy implements TemplateStrategy {
    private final IndexTemplateAPIWrapper<TemplateMapping> indexTemplateAPIWrapper;

    public V1TemplateStrategy(final IndexTemplateAPIWrapper<TemplateMapping> indexTemplateAPIWrapper) {
        this.indexTemplateAPIWrapper = indexTemplateAPIWrapper;
    }

    @Override
    public Optional<Long> getExistingTemplateVersion(final String templateName) throws IOException {
        return indexTemplateAPIWrapper.getTemplate(templateName)
                .map(TemplateMapping::version);
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
