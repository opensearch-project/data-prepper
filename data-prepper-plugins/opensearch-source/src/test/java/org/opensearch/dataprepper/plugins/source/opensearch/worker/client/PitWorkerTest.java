/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.OpenSearchIndexPartitionCreationSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.PitWorker;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PitWorkerTest {

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    @Mock
    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;

    @Mock
    private SearchAccessor searchAccessor;

    @Mock
    private Buffer<Record<Event>> buffer;

    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        executorService = Executors.newSingleThreadExecutor();
    }

    private PitWorker createObjectUnderTest() {
        return new PitWorker(searchAccessor, openSearchSourceConfiguration, sourceCoordinator, buffer, openSearchIndexPartitionCreationSupplier);
    }

    @Test
    void run_with_getNextPartition_returning_empty_will_sleep_and_exit_when_interrupted() throws InterruptedException {
        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.empty());


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));
    }

    @Test
    void run_with_getNextPartition_with_non_empty_partition_closes_that_partition() throws InterruptedException {
        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition));

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getJobCount()).thenReturn(1);
        when(schedulingParameterConfiguration.getRate()).thenReturn(Duration.ZERO);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        doNothing().when(sourceCoordinator).closePartition(partitionKey,
                Duration.ZERO, 1);


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));
    }
}
