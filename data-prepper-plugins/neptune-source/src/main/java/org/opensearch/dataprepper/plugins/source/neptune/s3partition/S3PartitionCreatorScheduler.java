package org.opensearch.dataprepper.plugins.source.neptune.s3partition;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.S3FolderPartition;
import org.opensearch.dataprepper.plugins.source.neptune.model.S3PartitionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class S3PartitionCreatorScheduler extends S3FolderPartitionCoordinator implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(S3PartitionCreatorScheduler.class);
    public static final String S3_FOLDER_PREFIX = "S3-FOLDER-";
    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;

    private final EnhancedSourceCoordinator sourceCoordinator;

    public S3PartitionCreatorScheduler(final EnhancedSourceCoordinator sourceCoordinator) {
        super(sourceCoordinator);
        this.sourceCoordinator = sourceCoordinator;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(S3FolderPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    final S3FolderPartition s3FolderPartition = (S3FolderPartition) sourcePartition.get();
                    final List<String> s3Folders = createS3BucketPartitions(s3FolderPartition.getPartitionCount());
                    sourceCoordinator.completePartition(s3FolderPartition);
                    final S3PartitionStatus s3PartitionStatus = new S3PartitionStatus(s3Folders);
                    sourceCoordinator.createPartition(new GlobalState(S3_FOLDER_PREFIX, s3PartitionStatus.toMap()));
                    break;
                }

                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The S3 partition creator scheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }

                final Optional<S3PartitionStatus> s3PartitionStatus = getGlobalS3FolderCreationStatus();
                if (s3PartitionStatus.isPresent()) {
                    LOG.info("The S3 folder partition global state created.");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception during creation of S3 partition folder, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The S3 partition creator scheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
    }

    private List<String> createS3BucketPartitions(int partitionCount) {
        final S3PartitionCreator s3PartitionCreator = new S3PartitionCreator(partitionCount);
        return s3PartitionCreator.createPartition();
    }
}
