/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;

import io.micrometer.core.instrument.Counter;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class WorkerCommonUtilsCompletionTest {

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private SchedulingParameterConfiguration schedulingParameterConfiguration;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;

    @Mock
    private SourcePartition<OpenSearchIndexProgressState> indexPartition;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    private OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;

    private String partitionKey;

    @BeforeEach
    void setup() {
        partitionKey = UUID.randomUUID().toString();
        lenient().when(indexPartition.getPartitionKey()).thenReturn(partitionKey);
        lenient().when(openSearchSourceConfiguration.getSchedulingParameterConfiguration())
                .thenReturn(schedulingParameterConfiguration);
        openSearchSourcePluginMetrics = mock(OpenSearchSourcePluginMetrics.class);
        lenient().when(openSearchSourcePluginMetrics.getIndicesCompletedWithFailuresCounter()).thenReturn(mock(Counter.class));
    }

    @Test
    void completeIndexPartition_in_periodic_mode_without_acknowledgments_calls_closePartition() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(false);
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(false);
        when(schedulingParameterConfiguration.getInterval()).thenReturn(Duration.ofHours(8));
        when(schedulingParameterConfiguration.getIndexReadCount()).thenReturn(1);

        WorkerCommonUtils.completeIndexPartition(openSearchSourceConfiguration, acknowledgementSet,
                indexPartition, sourceCoordinator, openSearchSourcePluginMetrics);

        verify(sourceCoordinator).closePartition(eq(partitionKey), eq(Duration.ofHours(8)), eq(1), eq(false));
        verify(sourceCoordinator, never()).completePartition(eq(partitionKey), eq(false));
    }

    @Test
    void completeIndexPartition_in_single_scan_mode_without_acknowledgments_calls_completePartition() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(false);
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(true);

        WorkerCommonUtils.completeIndexPartition(openSearchSourceConfiguration, acknowledgementSet,
                indexPartition, sourceCoordinator, openSearchSourcePluginMetrics);

        verify(sourceCoordinator).completePartition(eq(partitionKey), eq(false));
        verify(sourceCoordinator, never()).closePartition(eq(partitionKey), any(Duration.class), any(Integer.class), any(Boolean.class));
    }

    @Test
    void completeIndexPartition_with_acknowledgments_delegates_to_ack_path_regardless_of_mode() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(true);

        WorkerCommonUtils.completeIndexPartition(openSearchSourceConfiguration, acknowledgementSet,
                indexPartition, sourceCoordinator, openSearchSourcePluginMetrics);

        verify(sourceCoordinator).updatePartitionForAcknowledgmentWait(eq(partitionKey), any(Duration.class));
        verify(acknowledgementSet).complete();
        verify(sourceCoordinator, never()).closePartition(eq(partitionKey), any(Duration.class), any(Integer.class), any(Boolean.class));
        verify(sourceCoordinator, never()).completePartition(eq(partitionKey), any(Boolean.class));
    }

    @Test
    void createAcknowledgmentSet_in_periodic_mode_invokes_closePartition_on_successful_ack() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(true);
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(false);
        when(schedulingParameterConfiguration.getInterval()).thenReturn(Duration.ofHours(8));
        when(schedulingParameterConfiguration.getIndexReadCount()).thenReturn(1);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);

        when(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class))).thenReturn(acknowledgementSet);

        WorkerCommonUtils.createAcknowledgmentSet(acknowledgementSetManager,
                openSearchSourceConfiguration, sourceCoordinator, indexPartition);

        callbackCaptor.getValue().accept(true);

        verify(sourceCoordinator).closePartition(eq(partitionKey), eq(Duration.ofHours(8)), eq(1), eq(true));
        verify(sourceCoordinator, never()).completePartition(eq(partitionKey), any(Boolean.class));
    }

    @Test
    void createAcknowledgmentSet_in_single_scan_mode_invokes_completePartition_on_successful_ack() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(true);
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(true);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);

        when(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class))).thenReturn(acknowledgementSet);

        WorkerCommonUtils.createAcknowledgmentSet(acknowledgementSetManager,
                openSearchSourceConfiguration, sourceCoordinator, indexPartition);

        callbackCaptor.getValue().accept(true);

        verify(sourceCoordinator).completePartition(eq(partitionKey), eq(true));
        verify(sourceCoordinator, never()).closePartition(eq(partitionKey), any(Duration.class), any(Integer.class), any(Boolean.class));
    }

    @Test
    void createAcknowledgmentSet_on_failed_ack_gives_up_partition_regardless_of_mode() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(true);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);

        when(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class))).thenReturn(acknowledgementSet);

        WorkerCommonUtils.createAcknowledgmentSet(acknowledgementSetManager,
                openSearchSourceConfiguration, sourceCoordinator, indexPartition);

        callbackCaptor.getValue().accept(false);

        verify(sourceCoordinator).giveUpPartition(eq(partitionKey));
        verify(sourceCoordinator, never()).completePartition(eq(partitionKey), any(Boolean.class));
        verify(sourceCoordinator, never()).closePartition(eq(partitionKey), any(Duration.class), any(Integer.class), any(Boolean.class));
    }

    @Test
    void completeIndexPartition_with_failures_recorded_in_progress_state_increments_completed_with_failures_counter() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(false);
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(true);

        final OpenSearchIndexProgressState progressState = new OpenSearchIndexProgressState();
        progressState.setHadSearchFailures(true);
        progressState.recordRequestFailure(new RuntimeException("boom"));
        when(indexPartition.getPartitionState()).thenReturn(java.util.Optional.of(progressState));

        final Counter indicesCompletedWithFailuresCounter = mock(Counter.class);
        when(openSearchSourcePluginMetrics.getIndicesCompletedWithFailuresCounter())
                .thenReturn(indicesCompletedWithFailuresCounter);

        WorkerCommonUtils.completeIndexPartition(openSearchSourceConfiguration, acknowledgementSet,
                indexPartition, sourceCoordinator, openSearchSourcePluginMetrics);

        verify(indicesCompletedWithFailuresCounter).increment();
        verify(sourceCoordinator).completePartition(eq(partitionKey), eq(false));
    }

    @Test
    void completeIndexPartition_without_failures_does_not_increment_completed_with_failures_counter() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(false);
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(true);

        final OpenSearchIndexProgressState progressState = new OpenSearchIndexProgressState();
        when(indexPartition.getPartitionState()).thenReturn(java.util.Optional.of(progressState));

        final Counter indicesCompletedWithFailuresCounter = mock(Counter.class);
        lenient().when(openSearchSourcePluginMetrics.getIndicesCompletedWithFailuresCounter())
                .thenReturn(indicesCompletedWithFailuresCounter);

        WorkerCommonUtils.completeIndexPartition(openSearchSourceConfiguration, acknowledgementSet,
                indexPartition, sourceCoordinator, openSearchSourcePluginMetrics);

        verify(indicesCompletedWithFailuresCounter, never()).increment();
        verify(sourceCoordinator).completePartition(eq(partitionKey), eq(false));
    }

    @Test
    void completeIndexPartition_without_progress_state_does_not_attempt_failure_summary() {
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(false);
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(true);

        when(indexPartition.getPartitionState()).thenReturn(java.util.Optional.empty());

        final Counter indicesCompletedWithFailuresCounter = mock(Counter.class);
        lenient().when(openSearchSourcePluginMetrics.getIndicesCompletedWithFailuresCounter())
                .thenReturn(indicesCompletedWithFailuresCounter);

        WorkerCommonUtils.completeIndexPartition(openSearchSourceConfiguration, acknowledgementSet,
                indexPartition, sourceCoordinator, openSearchSourcePluginMetrics);

        verify(indicesCompletedWithFailuresCounter, never()).increment();
    }
}
