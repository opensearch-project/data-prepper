package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;

public class BinlogClientWrapper implements ReplicationLogClient {

    private final BinaryLogClient binlogClient;

    public BinlogClientWrapper(final BinaryLogClient binlogClient) {
        this.binlogClient = binlogClient;
    }

    public void connect() throws IOException {
        binlogClient.connect();
    }

    public void disconnect() throws IOException {
        binlogClient.disconnect();
    }

    public BinaryLogClient getBinlogClient() {
        return binlogClient;
    }
}
