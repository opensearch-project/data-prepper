package org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.scheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.saas_crawler.base.SaasSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.state.SaasWorkerProgressState;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class WorkerSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private SaasSourceConfig sourceConfig;

    @Mock
    Buffer<Record<Event>> buffer;

    @Mock
    private Crawler crawler;

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;


    @Test
    void testUnableToAcquireLeaderPartition() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(buffer, coordinator, sourceConfig, crawler);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);
        Thread.sleep(100);
        executorService.shutdownNow();
        verifyNoInteractions(crawler);
    }

    @Test
    void testLeaderPartitionsCreation() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(buffer, coordinator, sourceConfig, crawler);

        String sourceId =  UUID.randomUUID() + "|" + SaasSourcePartition.PARTITION_TYPE;
        String state = "{\"keyAttributes\":{\"project\":\"project-1\"},\"totalItems\":0,\"loadedItems\":20,\"exportStartTime\":1729391235717,\"itemIds\":[\"GTMS-25\",\"GTMS-24\"]}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        PartitionFactory factory = new PartitionFactory();
        EnhancedSourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willReturn(Optional.of(sourcePartition));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        Thread.sleep(50);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        SaasWorkerProgressState stateObj = (SaasWorkerProgressState)sourcePartition.getProgressState().get();
        verify(crawler, atLeast(1)).executePartition(stateObj, buffer, sourceConfig);
        verify(coordinator, atLeast(1)).completePartition(eq(sourcePartition));
    }

    @Test
    void testEmptyProgressState() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(buffer, coordinator, sourceConfig, crawler);

        String sourceId =  UUID.randomUUID() + "|" + SaasSourcePartition.PARTITION_TYPE;
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
        WorkerScheduler workerScheduler = new WorkerScheduler(buffer, coordinator, sourceConfig, crawler);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        Thread.sleep(1000);
        executorService.shutdownNow();

        // Crawler shouldn't be invoked in this case
        verifyNoInteractions(crawler);
    }

    @Test
    void testWhenNoPartitionToWorkOn() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(buffer, coordinator, sourceConfig, crawler);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        //Wait for more than a minute as the default while loop wait time in leader scheduler is 1 minute
        Thread.sleep(11000);
        executorService.shutdownNow();

        // Crawler shouldn't be invoked in this case
        verifyNoInteractions(crawler);
    }

    @Test
    void testRetryBackOffTriggeredWhenExceptionOccurred() throws InterruptedException {
        WorkerScheduler workerScheduler = new WorkerScheduler(buffer, coordinator, sourceConfig, crawler);
        given(coordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(workerScheduler);

        //Wait for more than a minute as the default while loop wait time in leader scheduler is 1 minute
        Thread.sleep(11000);
        executorService.shutdownNow();

        // Crawler shouldn't be invoked in this case
        verifyNoInteractions(crawler);
    }
}
