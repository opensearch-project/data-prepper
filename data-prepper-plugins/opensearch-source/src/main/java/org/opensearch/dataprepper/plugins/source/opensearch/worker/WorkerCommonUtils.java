/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WorkerCommonUtils {

    static final int ACKNOWLEDGEMENT_SET_TIMEOUT_SECONDS = Integer.MAX_VALUE;

    static Pair<AcknowledgementSet, CompletableFuture<Boolean>> createAcknowledgmentSet(final AcknowledgementSetManager acknowledgementSetManager,
                                                                                               final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                                                               final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                                                                                               final SourcePartition<OpenSearchIndexProgressState> indexPartition) {
        AcknowledgementSet acknowledgementSet = null;
        CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

        if (openSearchSourceConfiguration.isAcknowledgmentsEnabled()) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result == true) {
                    sourceCoordinator.closePartition(
                            indexPartition.getPartitionKey(),
                            openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate(),
                            openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount());
                }
                completableFuture.complete(result);
            }, Duration.ofSeconds(ACKNOWLEDGEMENT_SET_TIMEOUT_SECONDS));
        }

        return Pair.of(acknowledgementSet, completableFuture);
    }

    static void completeIndexPartition(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                              final AcknowledgementSet acknowledgementSet,
                                              final CompletableFuture<Boolean> completableFuture,
                                              final SourcePartition<OpenSearchIndexProgressState> indexPartition,
                                              final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator) throws ExecutionException, InterruptedException, TimeoutException {
        if (openSearchSourceConfiguration.isAcknowledgmentsEnabled()) {
            acknowledgementSet.complete();
            completableFuture.get(ACKNOWLEDGEMENT_SET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } else {
            sourceCoordinator.closePartition(
                    indexPartition.getPartitionKey(),
                    openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate(),
                    openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount());
        }
    }
}
