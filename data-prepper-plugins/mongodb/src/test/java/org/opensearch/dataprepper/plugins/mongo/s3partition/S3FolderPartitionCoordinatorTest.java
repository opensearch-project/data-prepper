package org.opensearch.dataprepper.plugins.mongo.s3partition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class S3FolderPartitionCoordinatorTest {
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @InjectMocks
    private S3FolderPartitionCoordinator s3FolderPartitionCoordinator;

    @Test
    public void getGlobalS3FolderCreationStatus_empty() {
        final String collection = UUID.randomUUID().toString();
        when(sourceCoordinator.getPartition(S3PartitionCreatorScheduler.S3_FOLDER_PREFIX + collection)).thenReturn(Optional.empty());
        Optional<S3PartitionStatus>  partitionStatus = s3FolderPartitionCoordinator.getGlobalS3FolderCreationStatus(collection);
        assertThat(partitionStatus.isEmpty(), is(true));
    }

    @Test
    public void getGlobalS3FolderCreationStatus_nonEmpty() {
        final String collection = UUID.randomUUID().toString();
        final List<String> partitions = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final GlobalState globalState = mock(GlobalState.class);
        final Map<String, Object> props = Map.of("partitions", partitions);
        when(globalState.getProgressState()).thenReturn(Optional.of(props));
        when(sourceCoordinator.getPartition(S3PartitionCreatorScheduler.S3_FOLDER_PREFIX + collection)).thenReturn(Optional.of(globalState));
        Optional<S3PartitionStatus> partitionStatus = s3FolderPartitionCoordinator.getGlobalS3FolderCreationStatus(collection);
        assertThat(partitionStatus.isEmpty(), is(false));
        assertThat(partitionStatus.get().getPartitions(), is(partitions));
    }
}
