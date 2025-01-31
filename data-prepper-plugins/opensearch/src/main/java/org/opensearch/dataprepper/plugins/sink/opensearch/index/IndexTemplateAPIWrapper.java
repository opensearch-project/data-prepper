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
     * @throws IOException io exception
     */
    void putTemplate(IndexTemplate indexTemplate) throws IOException;

    /**
     * Retrieve the existing index template
     *
     * @param name The index template name to retrieve by.
     * @throws IOException io exception
     * @return an {@code Optional} containing the template if found, otherwise an empty {@code Optional}.
     */
    Optional<T> getTemplate(String name) throws IOException;
}
