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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class BinlogClientWrapper implements ReplicationLogClient {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogClientWrapper.class);
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
        List<BinaryLogClient.EventListener> eventListenerList = binlogClient.getEventListeners();
        if (!eventListenerList.isEmpty()) {
            for (BinaryLogClient.EventListener eventListener : eventListenerList) {
                if (eventListener instanceof BinlogEventListener) {
                    LOG.debug("Stopping checkpoint manager.");
                    ((BinlogEventListener) eventListener).stopCheckpointManager();
                }
                LOG.debug("Unregistering binlog event listeners.");
                binlogClient.unregisterEventListener(eventListener);
            }
        }

        LOG.debug("Unregistering binlog client lifecycle listeners.");
        binlogClient.getLifecycleListeners().forEach(binlogClient::unregisterLifecycleListener);

        binlogClient.disconnect();
    }

    public BinaryLogClient getBinlogClient() {
        return binlogClient;
    }
}
