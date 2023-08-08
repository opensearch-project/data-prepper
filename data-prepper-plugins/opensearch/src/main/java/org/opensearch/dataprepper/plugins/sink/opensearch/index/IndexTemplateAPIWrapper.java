package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.io.IOException;
import java.util.Optional;

/**
 * Client wrapper for calling index template APIs.
 */
public interface IndexTemplateAPIWrapper<T> {
    /**
     * Create or update the index template
     *
     * @param indexTemplate The {@link IndexTemplate} to create or update.
     */
    void putTemplate(IndexTemplate indexTemplate) throws IOException;

    /**
     * Retrieve the existing index template
     *
     * @param name The index template name to retrieve by.
     */
    Optional<T> getTemplate(String name) throws IOException;
}
