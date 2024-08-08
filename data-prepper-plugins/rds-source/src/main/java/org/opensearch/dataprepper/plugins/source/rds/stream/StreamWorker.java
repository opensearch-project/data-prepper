/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class StreamWorker {
    private static final Logger LOG = LoggerFactory.getLogger(StreamWorker.class);

    private static final int DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS = 60_000;

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final BinaryLogClient binaryLogClient;
    private final PluginMetrics pluginMetrics;

    StreamWorker(final EnhancedSourceCoordinator sourceCoordinator,
                 final BinaryLogClient binaryLogClient,
                 final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.binaryLogClient = binaryLogClient;
        this.pluginMetrics = pluginMetrics;
    }

    public static StreamWorker create(final EnhancedSourceCoordinator sourceCoordinator,
                                      final BinaryLogClient binaryLogClient,
                                      final PluginMetrics pluginMetrics) {
        return new StreamWorker(sourceCoordinator, binaryLogClient, pluginMetrics);
    }

    public void processStream(final StreamPartition streamPartition) {
        while (shouldWaitForExport(streamPartition) && !Thread.currentThread().isInterrupted()) {
            LOG.info("Initial load not completed yet for {}, waiting...", streamPartition.getPartitionKey());
            try {
                Thread.sleep(DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS);
            } catch (final InterruptedException ex) {
                LOG.info("The Stream Scheduler was interrupted while waiting to retry, stopping processing");
                Thread.currentThread().interrupt();
                break;
            }
        }

        setStartBinlogPosition(streamPartition);

        try {
            LOG.info("Connect to database to read change events.");
            binaryLogClient.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                binaryLogClient.disconnect();
            } catch (IOException e) {
                LOG.error("Binary log client failed to disconnect.", e);
            }
        }
    }

    private boolean shouldWaitForExport(final StreamPartition streamPartition) {
        if (!streamPartition.getProgressState().get().shouldWaitForExport()) {
            LOG.debug("Export is not enabled. Proceed with streaming.");
            return false;
        }

        return !isExportDone(streamPartition);
    }

    private boolean isExportDone(StreamPartition streamPartition) {
        final String dbIdentifier = streamPartition.getPartitionKey();
        Optional<EnhancedSourcePartition> globalStatePartition = sourceCoordinator.getPartition("stream-for-" + dbIdentifier);
        return globalStatePartition.isPresent();
    }

    private void setStartBinlogPosition(final StreamPartition streamPartition) {
        final BinlogCoordinate startBinlogPosition = streamPartition.getProgressState().get().getStartPosition();

        // set start of binlog stream to current position if exists
        if (startBinlogPosition != null) {
            final String binlogFilename = startBinlogPosition.getBinlogFilename();
            final long binlogPosition = startBinlogPosition.getBinlogPosition();
            LOG.debug("Will start binlog stream from binlog file {} and position {}.", binlogFilename, binlogPosition);
            binaryLogClient.setBinlogFilename(binlogFilename);
            binaryLogClient.setBinlogPosition(binlogPosition);
        }
    }
}
