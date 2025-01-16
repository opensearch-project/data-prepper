/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.io.IOException;

public class BinlogClientWrapper implements ReplicationLogClient {

    private final BinaryLogClient binlogClient;

    public BinlogClientWrapper(final BinaryLogClient binlogClient) {
        this.binlogClient = binlogClient;
    }

    @Override
    public void connect() throws IOException {
        binlogClient.connect();
    }

    @Override
    public void disconnect() throws IOException {
        binlogClient.disconnect();
    }

    public BinaryLogClient getBinlogClient() {
        return binlogClient;
    }
}
