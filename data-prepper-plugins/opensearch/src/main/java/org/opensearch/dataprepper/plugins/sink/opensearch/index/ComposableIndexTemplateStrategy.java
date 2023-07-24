/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.indices.get_index_template.IndexTemplateItem;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link TemplateStrategy} for the OpenSearch <a href="https://opensearch.org/docs/latest/im-plugin/index-templates/">index template</a>.
 */
class ComposableIndexTemplateStrategy implements TemplateStrategy {
    private final IndexTemplateAPIWrapper<IndexTemplateItem> indexTemplateAPIWrapper;

    public ComposableIndexTemplateStrategy(final IndexTemplateAPIWrapper<IndexTemplateItem> indexTemplateAPIWrapper) {
        this.indexTemplateAPIWrapper = indexTemplateAPIWrapper;
    }

    @Override
    public Optional<Long> getExistingTemplateVersion(final String templateName) throws IOException {
        return indexTemplateAPIWrapper.getTemplate(templateName)
                .map(IndexTemplateItem::indexTemplate)
                .map(indexTemplate -> indexTemplate.version());
    }

    @Override
    public IndexTemplate createIndexTemplate(final Map<String, Object> templateMap) {
        return new ComposableIndexTemplate(templateMap);
    }

    @Override
    public void createTemplate(final IndexTemplate indexTemplate) throws IOException {
        indexTemplateAPIWrapper.putTemplate(indexTemplate);
    }
}
