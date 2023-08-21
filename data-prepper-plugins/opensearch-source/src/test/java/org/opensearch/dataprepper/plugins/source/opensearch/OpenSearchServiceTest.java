/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.NoSearchContextWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.OpenSearchIndexPartitionCreationSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.PitWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.ScrollWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.SearchWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchService.BUFFER_TIMEOUT;

@ExtendWith(MockitoExtension.class)
public class OpenSearchServiceTest {

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private OpenSearchAccessor openSearchAccessor;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    @Mock
    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    @Mock
    private OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    @Mock
    private SearchWorker searchWorker;

    @BeforeEach
    void setup() {
        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(1000);

        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);
    }

    private OpenSearchService createObjectUnderTest() {
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
             final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class);
             final MockedConstruction<OpenSearchIndexPartitionCreationSupplier> mockedConstruction = mockConstruction(OpenSearchIndexPartitionCreationSupplier.class, (mock, context) -> {
                 openSearchIndexPartitionCreationSupplier = mock;
             })) {
            executorsMockedStatic.when(Executors::newSingleThreadScheduledExecutor).thenReturn(scheduledExecutorService);
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, openSearchSourceConfiguration.getSearchConfiguration().getBatchSize(), BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
            return OpenSearchService.createOpenSearchService(openSearchAccessor, sourceCoordinator, openSearchSourceConfiguration, buffer);
        }
    }

    @Test
    void source_coordinator_is_initialized_on_construction() {
        createObjectUnderTest();
        verify(sourceCoordinator).initialize();
    }

    @Test
    void search_context_types_get_submitted_correctly_for_point_in_time_and_executor_service_with_start_time_in_the_future() {
        when(openSearchAccessor.getSearchContextType()).thenReturn(SearchContextType.POINT_IN_TIME);
        final Instant startTime = Instant.now().plusSeconds(60);

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getStartTime()).thenReturn(startTime);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        try (final MockedConstruction<PitWorker> pitWorkerMockedConstruction = mockConstruction(PitWorker.class, (pitWorker, context) -> {
            searchWorker = pitWorker;
        })) {}

        createObjectUnderTest().start();

        final ArgumentCaptor<Long> argumentCaptor = ArgumentCaptor.forClass(Long.class);

        verify(scheduledExecutorService).schedule(ArgumentMatchers.any(Runnable.class), argumentCaptor.capture(), eq(TimeUnit.MILLISECONDS));

        final long waitTimeMillis = argumentCaptor.getValue();
        assertThat(waitTimeMillis, lessThanOrEqualTo(Duration.ofSeconds(60).toMillis()));
        assertThat(waitTimeMillis, greaterThan(Duration.ofSeconds(30).toMillis()));
    }

    @Test
    void search_context_types_get_submitted_correctly_for_scroll_and_executor_service_with_start_time_in_the_past() {
        when(openSearchAccessor.getSearchContextType()).thenReturn(SearchContextType.SCROLL);
        final Instant startTime = Instant.now().minusSeconds(60);

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getStartTime()).thenReturn(startTime);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        try (final MockedConstruction<ScrollWorker> scrollWorkerMockedConstruction = mockConstruction(ScrollWorker.class, (scrollWorker, context) -> {
            searchWorker = scrollWorker;
        })) {}

        createObjectUnderTest().start();

        verify(scheduledExecutorService).schedule(ArgumentMatchers.any(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void search_context_types_get_submitted_correctly_for_no_search_context_and_executor_service_with_start_time_in_the_past() {
        when(openSearchAccessor.getSearchContextType()).thenReturn(SearchContextType.NONE);
        final Instant startTime = Instant.now().minusSeconds(60);

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getStartTime()).thenReturn(startTime);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        try (final MockedConstruction<NoSearchContextWorker> noSearchContextWorkerMockedConstruction = mockConstruction(NoSearchContextWorker.class, (noSearchContextWorker, context) -> {
            searchWorker = noSearchContextWorker;
        })) {}

        createObjectUnderTest().start();

        verify(scheduledExecutorService).schedule(ArgumentMatchers.any(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
    }
}
