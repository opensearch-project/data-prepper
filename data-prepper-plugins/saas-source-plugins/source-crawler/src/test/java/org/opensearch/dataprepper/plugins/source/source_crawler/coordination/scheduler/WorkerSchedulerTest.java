package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import io.micrometer.core.instrument.Counter;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.WorkerScheduler.WORKER_PARTITIONS_FAILED;
import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.WorkerScheduler.WORKER_PARTITIONS_COMPLETED;
import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.WorkerScheduler.ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.WorkerScheduler.ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME;

@ExtendWith(MockitoExtension.class)
public class WorkerSchedulerTest {

    private final String pluginName = "sampleTestPlugin";
    @Mock
    Buffer<Record<Event>> buffer;
    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private CrawlerSourceConfig sourceConfig;
    @Mock
    private Crawler crawler;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private AcknowledgementSet acknowledgementSet;
    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    @Test
    void testUnableToAcquireLeaderPartition() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(pluginName, buffer,
                coordinator, sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);
        Thread.sleep(100);
        executorService.shutdownNow();
        verifyNoInteractions(crawler);
    }

    @Test
    void testDeserializePaginationCrawlerWorkerProgressState() throws Exception {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState\",\n" +
                "  \"keyAttributes\": {\"project\": \"project-1\"},\n" +
                "  \"totalItems\": 10,\n" +
                "  \"loadedItems\": 5,\n" +
                "  \"exportStartTime\": 1729391235717,\n" +
                "  \"itemIds\": [\"GTMS-25\", \"GTMS-24\"]\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(PaginationCrawlerWorkerProgressState.class);
        mapper.registerModule(new JavaTimeModule());

        SaasWorkerProgressState state = mapper.readValue(json, SaasWorkerProgressState.class);

        assertTrue(state instanceof PaginationCrawlerWorkerProgressState);
        PaginationCrawlerWorkerProgressState paginationCrawlerState = (PaginationCrawlerWorkerProgressState) state;

        assertEquals(10, paginationCrawlerState.getTotalItems());
        assertEquals(5, paginationCrawlerState.getLoadedItems());
        assertEquals("project-1", paginationCrawlerState.getKeyAttributes().get("project"));
        assertEquals(2, paginationCrawlerState.getItemIds().size());
    }

    @Test
    void testDeserializeCrowdStrikeWorkerProgressState() throws Exception {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState\",\n" +
                "  \"startTime\": 1729391235717,\n" +
                "  \"endTime\": 1729395235717,\n" +
                "  \"marker\": \"2717455246896ffb25d9fcba251f7afa6dad7c1f1ffa\"\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(CrowdStrikeWorkerProgressState.class);
        mapper.registerModule(new JavaTimeModule());

        SaasWorkerProgressState state = mapper.readValue(json, SaasWorkerProgressState.class);

        assertTrue(state instanceof CrowdStrikeWorkerProgressState);
        CrowdStrikeWorkerProgressState csState = (CrowdStrikeWorkerProgressState) state;

        assertEquals("2717455246896ffb25d9fcba251f7afa6dad7c1f1ffa", csState.getMarker());
        assertNotNull(csState.getStartTime());
        assertNotNull(csState.getEndTime());
    }

    @Test
    void testDeserializeDimensionalTimeSliceWorkerProgressState() throws Exception {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState\",\n" +
                "  \"startTime\": 1729391235717,\n" +
                "  \"endTime\": 1729395235717,\n" +
                "  \"dimensionType\": \"Exchange\"\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerSubtypes(DimensionalTimeSliceWorkerProgressState.class);
        mapper.registerModule(new JavaTimeModule());

        SaasWorkerProgressState state = mapper.readValue(json, SaasWorkerProgressState.class);

        assertTrue(state instanceof DimensionalTimeSliceWorkerProgressState);
        DimensionalTimeSliceWorkerProgressState dimensionalState =
                (DimensionalTimeSliceWorkerProgressState) state;

        assertNotNull(dimensionalState.getStartTime());
        assertNotNull(dimensionalState.getEndTime());
        assertEquals("Exchange", dimensionalState.getDimensionType());
    }


    @Test
    void testEmptyProgressState() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(pluginName, buffer,
                coordinator, sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);

        String sourceId = UUID.randomUUID() + "|" + SaasSourcePartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(null);
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        PartitionFactory factory = new PartitionFactory();
        EnhancedSourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willReturn(Optional.of(sourcePartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        Thread.sleep(50);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verifyNoInteractions(crawler);
        verify(coordinator, atLeast(1)).completePartition(eq(sourcePartition));
    }

    @Test
    void testExceptionWhileAcquiringWorkerPartition() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(pluginName, buffer,
                coordinator, sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        Thread.sleep(10);
        executorService.shutdownNow();

        // Crawler shouldn't be invoked in this case
        verifyNoInteractions(crawler);
    }

    @Test
    void testWhenNoPartitionToWorkOn() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(pluginName, buffer,
                coordinator, sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        //Wait for more than a minute as the default while loop wait time in leader scheduler is 1 minute
        Thread.sleep(10);
        executorService.shutdownNow();

        // Crawler shouldn't be invoked in this case
        verifyNoInteractions(crawler);
    }

    @Test
    void testRetryBackOffTriggeredWhenExceptionOccurred() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(pluginName, buffer,
                coordinator, sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        //Wait for more than a minute as the default while loop wait time in leader scheduler is 1 minute
        Thread.sleep(10);
        executorService.shutdownNow();

        // Crawler shouldn't be invoked in this case
        verifyNoInteractions(crawler);
    }

    @Test
    void testCompletePartitionWithAcknowledgements() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(pluginName, buffer,
                coordinator, sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);
        WorkerScheduler spyWorkerScheduler = org.mockito.Mockito.spy(workerScheduler);
        
        when(sourceConfig.isAcknowledgments()).thenReturn(true);
        
        PaginationCrawlerWorkerProgressState mockProgressState = new PaginationCrawlerWorkerProgressState();
        
        EnhancedSourcePartition mockPartition = org.mockito.Mockito.mock(EnhancedSourcePartition.class);
        when(mockPartition.getProgressState()).thenReturn(Optional.of(mockProgressState));
        
        org.mockito.Mockito.doAnswer(invocation -> {
            coordinator.completePartition(mockPartition);
            return acknowledgementSet;
        }).when(spyWorkerScheduler).createAcknowledgementSet(any(EnhancedSourcePartition.class));
        
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE))
            .willReturn(Optional.of(mockPartition))
            .willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(spyWorkerScheduler);

        Thread.sleep(500);
        executorService.shutdownNow();
        
        verify(mockPartition, atLeast(1)).getProgressState();
        verify(sourceConfig, atLeast(1)).isAcknowledgments();
        
        // Verify that createAcknowledgementSet was called
        verify(spyWorkerScheduler, times(1)).createAcknowledgementSet(eq(mockPartition));
        
        // Verify that crawler.executePartition was called with acknowledgement set
        verify(crawler, times(1)).executePartition(any(SaasWorkerProgressState.class), eq(buffer), eq(acknowledgementSet));
        
        // Verify that coordinator.completePartition was called (from the acknowledgement callback with true)
        verify(coordinator, times(1)).completePartition(eq(mockPartition));
    }

    @Test
    void testRetryableAndNonRetryableSaaSCrawlerExceptions() throws InterruptedException {
            // Given
            Counter mockFailedCounter = mock(Counter.class);
            Counter mockCompletedCounter = mock(Counter.class);
            Counter mockAckSuccessCounter = mock(Counter.class);
            Counter mockAckFailureCounter = mock(Counter.class);
    
            when(pluginMetrics.counter(WORKER_PARTITIONS_FAILED)).thenReturn(mockFailedCounter);
            when(pluginMetrics.counter(WORKER_PARTITIONS_COMPLETED)).thenReturn(mockCompletedCounter);
            when(pluginMetrics.counter(ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME)).thenReturn(mockAckSuccessCounter);
            when(pluginMetrics.counter(ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME)).thenReturn(mockAckFailureCounter);;
    
            WorkerScheduler workerScheduler = new WorkerScheduler(pluginName, buffer,
                    coordinator, sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);
    
            // Mock partition and state
            DimensionalTimeSliceWorkerProgressState mockProgressState = new DimensionalTimeSliceWorkerProgressState();
            mockProgressState.setPartitionCreationTime(Instant.now());
            SaasSourcePartition mockPartition = org.mockito.Mockito.mock(SaasSourcePartition.class);
            when(mockPartition.getProgressState()).thenReturn(Optional.of(mockProgressState));
    
            // Set up coordinator to return our mock partition
            when(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE))
                    .thenReturn(Optional.of(mockPartition))
                    .thenReturn(Optional.empty());
    
            // Mock crawler to throw retryable exception
            doThrow(new SaaSCrawlerException("Retryable error", true))
                    .when(crawler).executePartition(any(), any(), any());
    
            // When
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(workerScheduler);
    
            Thread.sleep(500);
            executorService.shutdownNow();
    
            // Then - Verify retryable exception handling
            verify(coordinator, never()).saveProgressStateForPartition(any(), any());
            verify(coordinator, never()).giveUpPartition(any());
            verify(mockFailedCounter).increment();
    
            // Reset mocks for non-retryable test
            when(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE))
                    .thenReturn(Optional.of(mockPartition))
                    .thenReturn(Optional.empty());
            doThrow(new SaaSCrawlerException("Non-retryable error", false))
                    .when(crawler).executePartition(any(), any(), any());
    
            // When - Run again with non-retryable exception
            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(workerScheduler);
    
            Thread.sleep(500);
            executorService.shutdownNow();
    
            // Then - Verify non-retryable exception handling
            verify(coordinator, never()).saveProgressStateForPartition(any(), any());
            verify(coordinator, never()).giveUpPartition(any());
            verify(mockFailedCounter, times(2)).increment();
    }
}
