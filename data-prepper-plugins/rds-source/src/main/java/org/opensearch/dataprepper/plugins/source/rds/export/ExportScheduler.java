/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.rds.RdsClient;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExportScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ExportScheduler.class);

    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    private static final Duration DEFAULT_CLOSE_DURATION = Duration.ofMinutes(10);
    private static final int DEFAULT_MAX_CLOSE_COUNT = 36;
    private static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 5 * 60_000;
    private static final int DEFAULT_CHECK_STATUS_INTERVAL_MILLS = 30 * 1000;

    private final RdsClient rdsClient;

    private final PluginMetrics pluginMetrics;

    private final EnhancedSourceCoordinator sourceCoordinator;

    private final ExecutorService executor;

    public ExportScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final RdsClient rdsClient,
                           final PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.sourceCoordinator = sourceCoordinator;
        this.rdsClient = rdsClient;
        this.executor = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {

    }
}
