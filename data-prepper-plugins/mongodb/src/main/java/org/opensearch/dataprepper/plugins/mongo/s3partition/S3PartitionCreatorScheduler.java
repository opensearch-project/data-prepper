package org.opensearch.dataprepper.plugins.mongo.s3partition;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.S3Folder;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.S3Partition;
import org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class S3PartitionCreatorScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(S3PartitionCreatorScheduler.class);
    public static final String S3_FOLDER_PREFIX = "S3-FOLDER-";
    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    private final EnhancedSourceCoordinator sourceCoordinator;
    public S3PartitionCreatorScheduler(final EnhancedSourceCoordinator sourceCoordinator) {
        this.sourceCoordinator = sourceCoordinator;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(S3Partition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    final S3Partition s3Partition = (S3Partition) sourcePartition.get();
                    final List<String> s3Folders = createS3BucketPartitions(s3Partition);
                    sourceCoordinator.completePartition(s3Partition);
                    final S3PartitionStatus s3PartitionStatus = new S3PartitionStatus(s3Folders.size());
                    sourceCoordinator.createPartition(new GlobalState(S3_FOLDER_PREFIX + s3Partition.getCollection(), s3PartitionStatus.toMap()));
                }

                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The S3 partition creator scheduler was interrupted while waiting to retry, stopping processing");
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
        LOG.warn("S3 partition creator scheduler interrupted, looks like shutdown has triggered");
    }

    private List<String> createS3BucketPartitions(final S3Partition s3Partition) {
        final S3PartitionCreator s3PartitionCreator = new S3PartitionCreator(s3Partition.getBucketName(), s3Partition.getSubFolder(),
                s3Partition.getRegion());
        final List<String> s3Folders = s3PartitionCreator.createPartition();
        s3Folders.forEach(s3Folder -> sourceCoordinator.createPartition(new S3Folder(s3Partition.getCollection(),
                s3Partition.getBucketName(), s3Partition.getSubFolder(), s3Folder, s3Partition.getRegion())));
        return s3Folders;
    }
}
