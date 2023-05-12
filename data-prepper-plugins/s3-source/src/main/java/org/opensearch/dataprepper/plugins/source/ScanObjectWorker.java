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
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.utils.Pair;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class responsible for processing the s3 scan objects with the help of <code>S3ObjectWorker</code>
 * or <code>S3SelectWorker</code>.
 */
public class ScanObjectWorker implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(ScanObjectWorker.class);

    private static final String BUCKET_OBJECT_PARTITION_KEY_FORMAT = "%s|%s";
    private static final int STANDARD_BACKOFF_MILLIS = 30_000;

    private final S3Client s3Client;

    private final List<ScanOptions> scanOptionsBuilderList;

    private final S3ObjectHandler s3ObjectHandler;

    private final BucketOwnerProvider bucketOwnerProvider;

    private final SourceCoordinator<S3SourceProgressState> sourceCoordinator;

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
    }

    @Override
    public void run() {
        while (!shouldStopProcessing) {
            startProcessingObject(STANDARD_BACKOFF_MILLIS);
        }
    }

    // For testing
    void runWithoutInfiniteLoop() {
        startProcessingObject(10);
    }

    private void startProcessingObject(final int waitTimeMillis) {
        final Optional<SourcePartition<S3SourceProgressState>> objectToProcess = sourceCoordinator.getNextPartition(this::provideFilteredBucketKeyPartitions);

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

    private List<PartitionIdentifier> provideFilteredBucketKeyPartitions() {
        final List<PartitionIdentifier> objectsToProcess = new ArrayList<>();

        for (final ScanOptions scanOptions : scanOptionsBuilderList) {
            final List<String> excludeItems = new ArrayList<>();
            final S3ScanKeyPathOption s3ScanKeyPathOption = scanOptions.getS3ScanKeyPathOption();
            final ListObjectsV2Request.Builder listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(scanOptions.getBucket());
            bucketOwnerProvider.getBucketOwner(scanOptions.getBucket())
                    .ifPresent(listObjectsV2Request::expectedBucketOwner);

            if (Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions()))
                excludeItems.addAll(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions());

            if (Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3scanIncludeOptions()))
                s3ScanKeyPathOption.getS3scanIncludeOptions().forEach(includePath -> {
                    listObjectsV2Request.prefix(includePath);
                    objectsToProcess.addAll(listS3Objects(excludeItems, listObjectsV2Request, scanOptions.getBucket(),
                            scanOptions.getUseStartDateTime(), scanOptions.getUseEndDateTime()));
                });
            else
                objectsToProcess.addAll(listS3Objects(excludeItems, listObjectsV2Request, scanOptions.getBucket(),
                        scanOptions.getUseStartDateTime(), scanOptions.getUseEndDateTime()));
        }

        return objectsToProcess;
    }

    private List<PartitionIdentifier> listS3Objects(final List<String> excludeKeyPaths,
                                                    final ListObjectsV2Request.Builder listObjectsV2Request,
                                                    final String bucket,
                                                    final LocalDateTime startDateTime,
                                                    final LocalDateTime endDateTime) {
          return s3Client.listObjectsV2(listObjectsV2Request.fetchOwner(true).build()).contents().stream()
              .map(s3Object -> Pair.of(s3Object.key(), instantToLocalDateTime(s3Object.lastModified())))
              .filter(keyTimestampPair -> !keyTimestampPair.left().endsWith("/"))
              .filter(keyTimestampPair -> excludeKeyPaths.stream()
                    .noneMatch(excludeItem -> keyTimestampPair.left().endsWith(excludeItem)))
              .filter(keyTimestampPair -> isKeyMatchedBetweenTimeRange(keyTimestampPair.right(), startDateTime, endDateTime))
              .map(Pair::left)
              .map(objectKey -> PartitionIdentifier.builder().withPartitionKey(String.format(BUCKET_OBJECT_PARTITION_KEY_FORMAT, bucket, objectKey)).build())
              .collect(Collectors.toList());
    }

    private LocalDateTime instantToLocalDateTime(final Instant instant) {
        final ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
        return zonedDateTime.toLocalDateTime();
    }

    private void processS3Object(final S3ObjectReference s3ObjectReference){
        try {
            s3ObjectHandler.parseS3Object(s3ObjectReference,null);
        } catch (IOException ex) {
            LOG.error("Error while process the parseS3Object. ",ex);
        }
    }

    /**
     * Used for identifying s3 object last modified time match with scan the date range.
     * @return boolean
     */
    private boolean isKeyMatchedBetweenTimeRange(final LocalDateTime lastModifiedTime,
                                                final LocalDateTime startDateTime,
                                                final LocalDateTime endDateTime){
        return lastModifiedTime.isAfter(startDateTime) && lastModifiedTime.isBefore(endDateTime);
    }

}
