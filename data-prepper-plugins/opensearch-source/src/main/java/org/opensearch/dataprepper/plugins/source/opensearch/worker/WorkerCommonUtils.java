/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;

import static com.google.common.math.LongMath.pow;
import static com.google.common.primitives.Longs.min;
import static java.lang.Math.max;

public class WorkerCommonUtils {
    private static final Random RANDOM = new Random();
    private static final Logger LOG = LoggerFactory.getLogger(WorkerCommonUtils.class);

    static final Duration BACKOFF_ON_EXCEPTION = Duration.ofSeconds(60);

    static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);
    static final Duration STARTING_BACKOFF = Duration.ofMillis(500);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(60);
    static final int BACKOFF_RATE = 2;
    static final Duration MAX_JITTER = Duration.ofSeconds(2);
    static final Duration MIN_JITTER = Duration.ofSeconds(-2);

    static AcknowledgementSet createAcknowledgmentSet(final AcknowledgementSetManager acknowledgementSetManager,
                                                                                               final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                                                               final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                                                                                               final SourcePartition<OpenSearchIndexProgressState> indexPartition) {
        AcknowledgementSet acknowledgementSet = null;
        if (openSearchSourceConfiguration.isAcknowledgmentsEnabled()) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result == true) {
                    sourceCoordinator.closePartition(
                            indexPartition.getPartitionKey(),
                            openSearchSourceConfiguration.getSchedulingParameterConfiguration().getInterval(),
                            openSearchSourceConfiguration.getSchedulingParameterConfiguration().getIndexReadCount(),
                            true);

                    LOG.info("Received acknowledgment of completion from sink for index {}", indexPartition.getPartitionKey());
                }
            }, ACKNOWLEDGEMENT_SET_TIMEOUT);
        }

        return acknowledgementSet;
    }

    static void completeIndexPartition(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                              final AcknowledgementSet acknowledgementSet,
                                              final SourcePartition<OpenSearchIndexProgressState> indexPartition,
                                              final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator) {
        if (openSearchSourceConfiguration.isAcknowledgmentsEnabled()) {
            sourceCoordinator.updatePartitionForAcknowledgmentWait(indexPartition.getPartitionKey(), ACKNOWLEDGEMENT_SET_TIMEOUT);
            acknowledgementSet.complete();
        } else {
            sourceCoordinator.closePartition(
                    indexPartition.getPartitionKey(),
                    openSearchSourceConfiguration.getSchedulingParameterConfiguration().getInterval(),
                    openSearchSourceConfiguration.getSchedulingParameterConfiguration().getIndexReadCount(),
                    false);
            LOG.info("Completed processing of index {}", indexPartition.getPartitionKey());
        }
    }

    static long calculateExponentialBackoffAndJitter(final int retryCount) {
        final long jitterMillis = MIN_JITTER.toMillis() + RANDOM.nextInt((int) (MAX_JITTER.toMillis() - MIN_JITTER.toMillis() + 1));
        return max(1, min(STARTING_BACKOFF.toMillis() * pow(BACKOFF_RATE, retryCount - 1) + jitterMillis, MAX_BACKOFF.toMillis()));
    }
}
