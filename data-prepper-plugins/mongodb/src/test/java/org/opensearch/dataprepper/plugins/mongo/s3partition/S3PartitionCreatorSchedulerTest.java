package org.opensearch.dataprepper.plugins.mongo.s3partition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.S3FolderPartition;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.mongo.s3partition.S3PartitionCreatorScheduler.S3_FOLDER_PREFIX;

@ExtendWith(MockitoExtension.class)
public class S3PartitionCreatorSchedulerTest {
    @Mock
    private EnhancedSourceCoordinator coordinator;
    private S3PartitionCreatorScheduler s3PartitionCreatorScheduler;

    @BeforeEach
    public void setup() {
        s3PartitionCreatorScheduler = new S3PartitionCreatorScheduler(coordinator, List.of(UUID.randomUUID().toString()));
    }

    @Test
    void test_S3FolderPartition_empty() {
        given(coordinator.acquireAvailablePartition(S3FolderPartition.PARTITION_TYPE)).willReturn(Optional.empty());
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> s3PartitionCreatorScheduler.run());
        await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> verify(coordinator, never()).completePartition(any(EnhancedSourcePartition.class)));
        await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> verify(coordinator, never()).createPartition(any(EnhancedSourcePartition.class)));
        executorService.shutdownNow();
    }

    @Test
    void test_S3FolderPartition_exist() {
        final S3FolderPartition s3FolderPartition = mock(S3FolderPartition.class);
        given(s3FolderPartition.getPartitionCount()).willReturn(Math.abs(new Random().nextInt(100)));
        given(s3FolderPartition.getCollection()).willReturn(UUID.randomUUID().toString());
        given(coordinator.acquireAvailablePartition(S3FolderPartition.PARTITION_TYPE)).willReturn(Optional.of(s3FolderPartition));
        s3PartitionCreatorScheduler.run();
        verify(coordinator).completePartition(s3FolderPartition);
        final ArgumentCaptor<GlobalState> argumentCaptor = ArgumentCaptor.forClass(GlobalState.class);
        verify(coordinator).createPartition(argumentCaptor.capture());
        final GlobalState globalState = argumentCaptor.getValue();
        assertThat(globalState.getPartitionKey(), is(S3_FOLDER_PREFIX + s3FolderPartition.getCollection()));
        assertThat(globalState.getProgressState().get(), hasKey("partitions"));
        final List<String> partitions = (List<String>) globalState.getProgressState().get().get("partitions");
        assertThat(partitions, hasSize(s3FolderPartition.getPartitionCount()));
    }
}
