/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeleteScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollResponse;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.ScrollWorker.SCROLL_TIME_PER_BATCH;

@ExtendWith(MockitoExtension.class)
public class ScrollWorkerTest {

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    @Mock
    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;

    @Mock
    private SearchAccessor searchAccessor;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        executorService = Executors.newSingleThreadExecutor();
    }

    private ScrollWorker createObjectUnderTest() {
        return new ScrollWorker(searchAccessor, openSearchSourceConfiguration, sourceCoordinator, bufferAccumulator, openSearchIndexPartitionCreationSupplier);
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
    void run_with_getNextPartition_with_non_empty_partition_creates_and_deletes_scroll_and_closes_that_partition() throws Exception {
        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);

        final String scrollId = UUID.randomUUID().toString();
        final ArgumentCaptor<CreateScrollRequest> requestArgumentCaptor = ArgumentCaptor.forClass(CreateScrollRequest.class);
        final CreateScrollResponse createScrollResponse = mock(CreateScrollResponse.class);
        when(createScrollResponse.getScrollId()).thenReturn(scrollId);
        when(createScrollResponse.getDocuments()).thenReturn(List.of(mock(Event.class), mock(Event.class)));
        when(searchAccessor.createScroll(requestArgumentCaptor.capture())).thenReturn(createScrollResponse);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(2);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchScrollResponse searchScrollResponse = mock(SearchScrollResponse.class);
        when(searchScrollResponse.getScrollId()).thenReturn(scrollId);
        when(searchScrollResponse.getDocuments()).thenReturn(List.of(mock(Event.class), mock(Event.class)))
                .thenReturn(List.of(mock(Event.class), mock(Event.class))).thenReturn(List.of(mock(Event.class))).thenReturn(List.of(mock(Event.class)));

        final ArgumentCaptor<SearchScrollRequest> searchScrollRequestArgumentCaptor = ArgumentCaptor.forClass(SearchScrollRequest.class);
        when(searchAccessor.searchWithScroll(searchScrollRequestArgumentCaptor.capture())).thenReturn(searchScrollResponse);

        doNothing().when(bufferAccumulator).add(any(Record.class));
        doNothing().when(bufferAccumulator).flush();

        final ArgumentCaptor<DeleteScrollRequest> deleteRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteScrollRequest.class);
        doNothing().when(searchAccessor).deleteScroll(deleteRequestArgumentCaptor.capture());

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition)).thenReturn(Optional.empty());

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

        final CreateScrollRequest createScrollRequest = requestArgumentCaptor.getValue();
        assertThat(createScrollRequest, notNullValue());
        assertThat(createScrollRequest.getSize(), equalTo(2));
        assertThat(createScrollRequest.getIndex(), equalTo(partitionKey));
        assertThat(createScrollRequest.getScrollTime(), equalTo(SCROLL_TIME_PER_BATCH));

        verify(searchAccessor, times(2)).searchWithScroll(any(SearchScrollRequest.class));
        verify(sourceCoordinator, times(2)).saveProgressStateForPartition(eq(partitionKey), eq(null));

        final List<SearchScrollRequest> searchScrollRequests = searchScrollRequestArgumentCaptor.getAllValues();
        assertThat(searchScrollRequests.size(), equalTo(2));
        assertThat(searchScrollRequests.get(0), notNullValue());
        assertThat(searchScrollRequests.get(0).getScrollId(), equalTo(scrollId));
        assertThat(searchScrollRequests.get(0).getScrollTime(), equalTo(SCROLL_TIME_PER_BATCH));

        assertThat(searchScrollRequests.get(1), notNullValue());
        assertThat(searchScrollRequests.get(1).getScrollId(), equalTo(scrollId));
        assertThat(searchScrollRequests.get(1).getScrollTime(), equalTo(SCROLL_TIME_PER_BATCH));


        final DeleteScrollRequest deleteScrollRequest = deleteRequestArgumentCaptor.getValue();
        assertThat(deleteScrollRequest, notNullValue());
        assertThat(deleteScrollRequest.getScrollId(), equalTo(scrollId));
    }

    @Test
    void run_gives_up_partitions_and_waits_when_createScroll_throws_SearchContextLimitException() throws InterruptedException {
        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(2);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        when(searchAccessor.createScroll(any(CreateScrollRequest.class))).thenThrow(SearchContextLimitException.class);

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition));


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));

        verifyNoMoreInteractions(searchAccessor);
        verify(sourceCoordinator).giveUpPartitions();
        verify(sourceCoordinator, never()).closePartition(anyString(), any(Duration.class), anyInt());
    }

}
