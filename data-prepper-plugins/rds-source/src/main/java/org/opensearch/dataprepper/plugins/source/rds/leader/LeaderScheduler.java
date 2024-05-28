/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.leader;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);
    private static final int DEFAULT_EXTEND_LEASE_MINUTES = 3;
    private static final Duration DEFAULT_LEASE_INTERVAL = Duration.ofMinutes(1);
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final RdsSourceConfig sourceConfig;

    public LeaderScheduler(final EnhancedSourceCoordinator sourceCoordinator, final RdsSourceConfig sourceConfig) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void run() {

    }
}
