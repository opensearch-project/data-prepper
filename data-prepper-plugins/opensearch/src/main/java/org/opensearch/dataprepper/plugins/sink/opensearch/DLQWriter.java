package org.opensearch.dataprepper.plugins.sink.opensearch;

import java.io.IOException;

public interface DLQWriter {
    void write(final String content) throws IOException;

    void close() throws IOException;
}
