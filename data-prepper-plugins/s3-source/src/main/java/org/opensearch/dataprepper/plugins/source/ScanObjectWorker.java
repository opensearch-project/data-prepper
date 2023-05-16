/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Class responsible for processing the s3 scan objects with the help of <code>S3ObjectWorker</code>
 * or <code>S3SelectWorker</code>.
 */
public class ScanObjectWorker implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(ScanObjectWorker.class);

    private static final int STANDARD_BACKOFF_MILLIS = 30_000;

    private final S3Client s3Client;

    private final List<ScanOptions> scanOptionsBuilderList;

    private final S3ObjectHandler s3ObjectHandler;

    private final BucketOwnerProvider bucketOwnerProvider;

    private final SourceCoordinator<S3SourceProgressState> sourceCoordinator;

    private final Supplier<List<PartitionIdentifier>> partitionCreationSupplier;

    // Should there be a duration or time that is configured in the source to stop processing? Otherwise will only stop when data prepper is stopped
    private final boolean shouldStopProcessing = false;

    public ScanObjectWorker(final S3Client s3Client,
                            final List<ScanOptions> scanOptionsBuilderList,
                            final S3ObjectHandler s3ObjectHandler,
                            final BucketOwnerProvider bucketOwnerProvider,
                            final SourceCoordinator<S3SourceProgressState> sourceCoordinator){
        this.s3Client = s3Client;
        this.scanOptionsBuilderList = scanOptionsBuilderList;
        this.s3ObjectHandler= s3ObjectHandler;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.sourceCoordinator = sourceCoordinator;
        this.sourceCoordinator.initialize();

        this.partitionCreationSupplier = new S3ScanPartitionCreationSupplier(s3Client, bucketOwnerProvider, scanOptionsBuilderList);
    }

    @Override
    public void run() {
        while (!shouldStopProcessing) {
            startProcessingObject(STANDARD_BACKOFF_MILLIS);
        }
    }

    /**
     * For testing
     */
    void runWithoutInfiniteLoop() {
        startProcessingObject(10);
    }

    private void startProcessingObject(final int waitTimeMillis) {
        final Optional<SourcePartition<S3SourceProgressState>> objectToProcess = sourceCoordinator.getNextPartition(partitionCreationSupplier);

        if (objectToProcess.isEmpty()) {
            try {
                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }

        final String bucket = objectToProcess.get().getPartitionKey().split("\\|")[0];
        final String objectKey = objectToProcess.get().getPartitionKey().split("\\|")[1];

        try {
            processS3Object(S3ObjectReference.bucketAndKey(bucket, objectKey).build());
            sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey());
        } catch (final PartitionNotOwnedException | PartitionNotFoundException | PartitionUpdateException e) {
            LOG.warn("S3 scan object worker received an exception from the source coordinator. There is a potential for duplicate data from {}, giving up partition and getting next partition: {}", objectKey, e.getMessage());
            sourceCoordinator.giveUpPartitions();
        }
    }

    private void processS3Object(final S3ObjectReference s3ObjectReference){
        try {
            s3ObjectHandler.parseS3Object(s3ObjectReference,null);
        } catch (IOException ex) {
            LOG.error("Error while process the parseS3Object. ",ex);
        }
    }
}
