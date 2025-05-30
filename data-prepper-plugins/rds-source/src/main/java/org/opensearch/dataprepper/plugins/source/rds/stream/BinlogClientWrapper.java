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
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import org.opensearch.dataprepper.plugins.source.rds.utils.RdsSourceAggregateMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class BinlogClientWrapper implements ReplicationLogClient {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogClientWrapper.class);
    static final String CONNECTION_REFUSED = "Connection refused";
    static final String FAILED_TO_DETERMINE_BINLOG_FILENAME = "Failed to determine binlog filename";
    static final String ACCESS_DENIED = "Access denied";
    private final BinaryLogClient binlogClient;
    private final RdsSourceAggregateMetrics rdsSourceAggregateMetrics;

    public BinlogClientWrapper(final BinaryLogClient binlogClient, final RdsSourceAggregateMetrics rdsSourceAggregateMetrics) {
        this.binlogClient = binlogClient;
        this.rdsSourceAggregateMetrics = rdsSourceAggregateMetrics;
    }

    @Override
    public void connect() throws IOException {
        try {
            rdsSourceAggregateMetrics.getStreamApiInvocations().increment();
            binlogClient.connect();
        } catch (Exception e) {
            LOG.error("Failed to connect to replication stream due to {}", e.getMessage());
            categorizeError(e);
            throw e;
        }
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

    private void categorizeError(Exception e) {
        if (e instanceof AuthenticationException) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamAuthErrors().increment();
            LOG.error("Failed to connect to replication stream: Authentication failed. [{}]", e.getMessage());
        } else if (e.getCause() != null && e.getCause().getMessage() != null && e.getCause().getMessage().contains(CONNECTION_REFUSED)) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamServerNotFoundErrors().increment();
            LOG.error("Failed to connect to replication stream: Cannot connect to MySQL server. [{}]", e.getMessage());
        } else if (e.getMessage() != null && e.getMessage().contains(FAILED_TO_DETERMINE_BINLOG_FILENAME)) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamReplicationNotEnabledErrors().increment();
            LOG.error("Failed to connect to replication stream: Binary logging not enabled on the server. [{}]", e.getMessage());
        } else if (e.getMessage() != null && e.getMessage().contains(ACCESS_DENIED)) {
            rdsSourceAggregateMetrics.getStream4xxErrors().increment();
            rdsSourceAggregateMetrics.getStreamAccessDeniedErrors().increment();
            LOG.error("Failed to connect to replication stream: Insufficient privileges. [{}]", e.getMessage());
        } else {
            rdsSourceAggregateMetrics.getStream5xxErrors().increment();
            LOG.error("Failed to connect to replication stream. ", e);
        }
    }
}
