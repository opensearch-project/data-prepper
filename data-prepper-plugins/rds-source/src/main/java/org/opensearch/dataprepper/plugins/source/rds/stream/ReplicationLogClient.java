package org.opensearch.dataprepper.plugins.source.rds.stream;

import java.io.IOException;

public interface ReplicationLogClient {

    void connect() throws IOException;

    void disconnect() throws IOException;
}
