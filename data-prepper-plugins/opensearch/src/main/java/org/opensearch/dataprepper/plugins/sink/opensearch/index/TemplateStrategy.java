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
    /**
     * Gets the version of the template if it exists.
     *
     * @param templateName The name of the template
     * @return An {@link Optional} with the template version, if it exists.
     *         An empty {@link Optional} if the template does not exist.
     * @throws IOException An exception reading from OpenSearch.
     */
    Optional<Long> getExistingTemplateVersion(final String templateName) throws IOException;

    /**
     * Creates a new {@link IndexTemplate} object from a {@link Map} representation.
     * <p>
     * This does not create anything on the OpenSearch cluster.
     *
     * @param templateMap The map representation
     * @return The {@link IndexTemplate} standard representation of the template
     */
    IndexTemplate createIndexTemplate(Map<String, Object> templateMap);

    /**
     * Creates an index template on the OpenSeach cluster.
     *
     * @param indexConfiguration The index configuration
     * @param indexTemplate The standard representation for a template
     * @throws IOException An exception writing to OpenSearch
     */
    void createTemplate(IndexConfiguration indexConfiguration, IndexTemplate indexTemplate) throws IOException;
}
