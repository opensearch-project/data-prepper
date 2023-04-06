/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Provides a strategy for coordinating with OpenSearch for index templates.
 */
public interface TemplateStrategy {
    Optional<Long> getExistingTemplateVersion(final String templateName) throws IOException;

    IndexTemplate createIndexTemplate(Map<String, Object> templateMap);

    void createTemplate(IndexTemplate indexTemplate) throws IOException;
}
