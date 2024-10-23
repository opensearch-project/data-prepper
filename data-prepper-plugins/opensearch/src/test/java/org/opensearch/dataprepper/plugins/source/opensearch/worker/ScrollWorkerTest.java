/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.IndexNotFoundException;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.ScrollWorker.SCROLL_TIME_PER_BATCH;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.ACKNOWLEDGEMENT_SET_TIMEOUT;

@ExtendWith(MockitoExtension.class)
public class ScrollWorkerTest {
    @Mock
    private ObjectMapper objectMapper;

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

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock(lenient = true)
    private OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;

    @Mock
    private Counter documentsProcessedCounter;

    @Mock
    private Counter indicesProcessedCounter;

    @Mock
    private Counter processingErrorsCounter;

    @Mock
    private Timer indexProcessingTimeTimer;

    @Mock
    private DistributionSummary bytesReceivedSummary;

    @Mock
    private DistributionSummary bytesProcessedSummary;

    private ExecutorService executorService;

    @BeforeEach
    void setup() {
        executorService = Executors.newSingleThreadExecutor();
        lenient().when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(false);
        when(openSearchSourcePluginMetrics.getDocumentsProcessedCounter()).thenReturn(documentsProcessedCounter);
        when(openSearchSourcePluginMetrics.getIndicesProcessedCounter()).thenReturn(indicesProcessedCounter);
        when(openSearchSourcePluginMetrics.getProcessingErrorsCounter()).thenReturn(processingErrorsCounter);
        when(openSearchSourcePluginMetrics.getIndexProcessingTimeTimer()).thenReturn(indexProcessingTimeTimer);
        when(openSearchSourcePluginMetrics.getBytesReceivedSummary()).thenReturn(bytesReceivedSummary);
        when(openSearchSourcePluginMetrics.getBytesProcessedSummary()).thenReturn(bytesProcessedSummary);
    }

    private ScrollWorker createObjectUnderTest() {
        return new ScrollWorker(objectMapper, searchAccessor, openSearchSourceConfiguration, sourceCoordinator, bufferAccumulator, openSearchIndexPartitionCreationSupplier, acknowledgementSetManager, openSearchSourcePluginMetrics);
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
        mockTimerCallable();

        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);

        final String scrollId = UUID.randomUUID().toString();
        final ArgumentCaptor<CreateScrollRequest> requestArgumentCaptor = ArgumentCaptor.forClass(CreateScrollRequest.class);
        final CreateScrollResponse createScrollResponse = mock(CreateScrollResponse.class);
        when(createScrollResponse.getScrollId()).thenReturn(scrollId);
        final Event testEvent1 = mock(Event.class);
        final Event testEvent2 = mock(Event.class);
        final JsonNode testData1 = mock(JsonNode.class);
        final JsonNode testData2 = mock(JsonNode.class);
        when(testEvent1.getJsonNode()).thenReturn(testData1);
        when(testEvent2.getJsonNode()).thenReturn(testData2);
        when(objectMapper.writeValueAsBytes(testData1)).thenReturn(new byte[10]);
        when(objectMapper.writeValueAsBytes(testData2)).thenReturn(new byte[20]);
        when(createScrollResponse.getDocuments()).thenReturn(List.of(testEvent1, testEvent2));
        when(searchAccessor.createScroll(requestArgumentCaptor.capture())).thenReturn(createScrollResponse);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(2);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchScrollResponse searchScrollResponse = mock(SearchScrollResponse.class);
        when(searchScrollResponse.getScrollId()).thenReturn(scrollId);
        final Event testEvent3 = mock(Event.class);
        final Event testEvent4 = mock(Event.class);
        final Event testEvent5 = mock(Event.class);
        final JsonNode testData3 = mock(JsonNode.class);
        final JsonNode testData4 = mock(JsonNode.class);
        final JsonNode testData5 = mock(JsonNode.class);
        when(testEvent3.getJsonNode()).thenReturn(testData3);
        when(testEvent4.getJsonNode()).thenReturn(testData4);
        when(testEvent5.getJsonNode()).thenReturn(testData5);
        when(objectMapper.writeValueAsBytes(testData3)).thenReturn(new byte[30]);
        when(objectMapper.writeValueAsBytes(testData4)).thenReturn(new byte[40]);
        when(objectMapper.writeValueAsBytes(testData5)).thenReturn(new byte[50]);
        when(searchScrollResponse.getDocuments()).thenReturn(List.of(testEvent3, testEvent4))
                .thenReturn(List.of(testEvent3, testEvent4)).thenReturn(List.of(testEvent5)).thenReturn(List.of(testEvent5));

        final ArgumentCaptor<SearchScrollRequest> searchScrollRequestArgumentCaptor = ArgumentCaptor.forClass(SearchScrollRequest.class);
        when(searchAccessor.searchWithScroll(searchScrollRequestArgumentCaptor.capture())).thenReturn(searchScrollResponse);

        doNothing().when(bufferAccumulator).add(any(Record.class));
        doNothing().when(bufferAccumulator).flush();

        final ArgumentCaptor<DeleteScrollRequest> deleteRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteScrollRequest.class);
        doNothing().when(searchAccessor).deleteScroll(deleteRequestArgumentCaptor.capture());

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition)).thenReturn(Optional.empty());

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getIndexReadCount()).thenReturn(1);
        when(schedulingParameterConfiguration.getInterval()).thenReturn(Duration.ZERO);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        doNothing().when(sourceCoordinator).closePartition(partitionKey,
                Duration.ZERO, 1, false);


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
        verify(sourceCoordinator, times(0)).saveProgressStateForPartition(eq(partitionKey), eq(null));

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

        verify(bytesReceivedSummary).record(10L);
        verify(bytesReceivedSummary).record(20L);
        verify(bytesReceivedSummary).record(30L);
        verify(bytesReceivedSummary).record(40L);
        verify(bytesReceivedSummary).record(50L);
        verify(documentsProcessedCounter, times(5)).increment();
        verify(bytesProcessedSummary).record(10L);
        verify(bytesProcessedSummary).record(20L);
        verify(bytesProcessedSummary).record(30L);
        verify(bytesProcessedSummary).record(40L);
        verify(bytesProcessedSummary).record(50L);
        verify(indicesProcessedCounter).increment();
        verifyNoInteractions(processingErrorsCounter);
    }

    @Test
    void run_with_getNextPartition_with_acknowledgments_creates_and_deletes_scroll_and_closes_that_partition() throws Exception {
        mockTimerCallable();

        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        AtomicReference<Integer> numEventsAdded = new AtomicReference<>(0);
        doAnswer(a -> {
            numEventsAdded.getAndSet(numEventsAdded.get() + 1);
            return null;
        }).when(acknowledgementSet).add(any(Event.class));

        doAnswer(invocation -> {
            Consumer<Boolean> consumer = invocation.getArgument(0);
            consumer.accept(true);
            return acknowledgementSet;
        }).when(acknowledgementSetManager).create(any(Consumer.class), eq(ACKNOWLEDGEMENT_SET_TIMEOUT));
        when(openSearchSourceConfiguration.isAcknowledgmentsEnabled()).thenReturn(true);

        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);

        final String scrollId = UUID.randomUUID().toString();
        final ArgumentCaptor<CreateScrollRequest> requestArgumentCaptor = ArgumentCaptor.forClass(CreateScrollRequest.class);
        final CreateScrollResponse createScrollResponse = mock(CreateScrollResponse.class);
        when(createScrollResponse.getScrollId()).thenReturn(scrollId);
        final Event testEvent1 = mock(Event.class);
        final Event testEvent2 = mock(Event.class);
        final JsonNode testData1 = mock(JsonNode.class);
        final JsonNode testData2 = mock(JsonNode.class);
        when(testEvent1.getJsonNode()).thenReturn(testData1);
        when(testEvent2.getJsonNode()).thenReturn(testData2);
        when(objectMapper.writeValueAsBytes(testData1)).thenReturn(new byte[10]);
        when(objectMapper.writeValueAsBytes(testData2)).thenReturn(new byte[20]);
        when(createScrollResponse.getDocuments()).thenReturn(List.of(testEvent1, testEvent2));
        when(searchAccessor.createScroll(requestArgumentCaptor.capture())).thenReturn(createScrollResponse);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(2);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchScrollResponse searchScrollResponse = mock(SearchScrollResponse.class);
        when(searchScrollResponse.getScrollId()).thenReturn(scrollId);
        final Event testEvent3 = mock(Event.class);
        final Event testEvent4 = mock(Event.class);
        final Event testEvent5 = mock(Event.class);
        final JsonNode testData3 = mock(JsonNode.class);
        final JsonNode testData4 = mock(JsonNode.class);
        final JsonNode testData5 = mock(JsonNode.class);
        when(testEvent3.getJsonNode()).thenReturn(testData3);
        when(testEvent4.getJsonNode()).thenReturn(testData4);
        when(testEvent5.getJsonNode()).thenReturn(testData5);
        when(objectMapper.writeValueAsBytes(testData3)).thenReturn(new byte[30]);
        when(objectMapper.writeValueAsBytes(testData4)).thenReturn(new byte[40]);
        when(objectMapper.writeValueAsBytes(testData5)).thenReturn(new byte[50]);
        when(searchScrollResponse.getDocuments()).thenReturn(List.of(testEvent3, testEvent4))
                .thenReturn(List.of(testEvent3, testEvent4)).thenReturn(List.of(testEvent5)).thenReturn(List.of(testEvent5));

        final ArgumentCaptor<SearchScrollRequest> searchScrollRequestArgumentCaptor = ArgumentCaptor.forClass(SearchScrollRequest.class);
        when(searchAccessor.searchWithScroll(searchScrollRequestArgumentCaptor.capture())).thenReturn(searchScrollResponse);

        doNothing().when(bufferAccumulator).add(any(Record.class));
        doNothing().when(bufferAccumulator).flush();

        final ArgumentCaptor<DeleteScrollRequest> deleteRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteScrollRequest.class);
        doNothing().when(searchAccessor).deleteScroll(deleteRequestArgumentCaptor.capture());

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition)).thenReturn(Optional.empty());

        final SchedulingParameterConfiguration schedulingParameterConfiguration = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameterConfiguration.getIndexReadCount()).thenReturn(1);
        when(schedulingParameterConfiguration.getInterval()).thenReturn(Duration.ZERO);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameterConfiguration);

        doNothing().when(sourceCoordinator).closePartition(partitionKey,
                Duration.ZERO, 1, true);


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
        verify(sourceCoordinator, times(0)).saveProgressStateForPartition(eq(partitionKey), eq(null));

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

        verify(acknowledgementSet).complete();

        verify(bytesReceivedSummary).record(10L);
        verify(bytesReceivedSummary).record(20L);
        verify(bytesReceivedSummary).record(30L);
        verify(bytesReceivedSummary).record(40L);
        verify(bytesReceivedSummary).record(50L);
        verify(documentsProcessedCounter, times(5)).increment();
        verify(bytesProcessedSummary).record(10L);
        verify(bytesProcessedSummary).record(20L);
        verify(bytesProcessedSummary).record(30L);
        verify(bytesProcessedSummary).record(40L);
        verify(bytesProcessedSummary).record(50L);
        verify(indicesProcessedCounter).increment();
        verifyNoInteractions(processingErrorsCounter);
    }

    @Test
    void run_gives_up_partitions_and_waits_when_createScroll_throws_SearchContextLimitException() throws Exception {
        mockTimerCallable();

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
        verify(sourceCoordinator).giveUpPartition(sourcePartition.getPartitionKey());
        verify(sourceCoordinator, never()).closePartition(anyString(), any(Duration.class), anyInt(), eq(false));

        verifyNoInteractions(documentsProcessedCounter);
        verifyNoInteractions(indicesProcessedCounter);
        verify(processingErrorsCounter).increment();
    }


    @Test
    void run_completes_partitions_createScroll_throws_IndexNotFoundException() throws Exception {
        mockTimerCallable();

        final SourcePartition<OpenSearchIndexProgressState> sourcePartition = mock(SourcePartition.class);
        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartition.getPartitionKey()).thenReturn(partitionKey);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getBatchSize()).thenReturn(2);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        when(searchAccessor.createScroll(any(CreateScrollRequest.class))).thenThrow(IndexNotFoundException.class);

        when(sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier)).thenReturn(Optional.of(sourcePartition))
                .thenReturn(Optional.empty());


        final Future<?> future = executorService.submit(() -> createObjectUnderTest().run());
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(future.isCancelled(), equalTo(true));

        assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS), equalTo(true));

        verifyNoMoreInteractions(searchAccessor);
        verify(sourceCoordinator).completePartition(partitionKey, false);
        verify(sourceCoordinator, never()).closePartition(anyString(), any(Duration.class), anyInt(), anyBoolean());

        verifyNoInteractions(documentsProcessedCounter);
        verifyNoInteractions(indicesProcessedCounter);
        verifyNoInteractions(processingErrorsCounter);
    }

    private void mockTimerCallable() {
        doAnswer(a -> {
            a.<Runnable>getArgument(0).run();
            return null;
        }).when(indexProcessingTimeTimer).record(any(Runnable.class));
    }

}
