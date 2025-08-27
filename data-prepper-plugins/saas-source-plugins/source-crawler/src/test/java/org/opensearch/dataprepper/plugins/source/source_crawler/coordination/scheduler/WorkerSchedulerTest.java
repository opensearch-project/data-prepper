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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
}
