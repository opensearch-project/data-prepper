package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.io.IOException;
import java.util.Optional;

public interface IndexTemplateAPIWrapper<T> {
    void putTemplate(IndexTemplate indexTemplate) throws IOException;

    Optional<T> getTemplate(String name) throws IOException;
}
