/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.s3.configuration.FolderPartitioningOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanSchedulingOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.model.source.s3.S3ScanEnvironmentVariables.STOP_S3_SCAN_PROCESSING_PROPERTY;

/**
 * Class responsible for processing the s3 scan objects with the help of <code>S3ObjectWorker</code>
 * or <code>S3SelectWorker</code>.
 */
public class ScanObjectWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ScanObjectWorker.class);
    private static final Integer MAX_OBJECTS_PER_ACKNOWLEDGMENT_SET = 1;

    static final Duration CHECKPOINT_OWNERSHIP_INTERVAL = Duration.ofMinutes(2);

    static final Duration NO_OBJECTS_FOUND_BEFORE_PARTITION_DELETION_DURATION = Duration.ofHours(1);
    private static final int RETRY_BACKOFF_ON_EXCEPTION_MILLIS = 5_000;
    static final String ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME = "acknowledgementSetCallbackCounter";

    static final String NO_OBJECTS_FOUND_FOR_FOLDER_PARTITION = "folderPartitionNoObjectsFound";

    static final String PARTITION_OWNERSHIP_UPDATE_ERRORS = "partitionOwnershipUpdateErrors";
    private final S3Client s3Client;

    private final List<ScanOptions> scanOptionsBuilderList;

    private final S3ObjectHandler s3ObjectHandler;

    private final BucketOwnerProvider bucketOwnerProvider;

    private final SourceCoordinator<S3SourceProgressState> sourceCoordinator;

    private final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier;

    private final S3ScanSchedulingOptions s3ScanSchedulingOptions;

    private final boolean endToEndAcknowledgementsEnabled;
    private final AcknowledgementSetManager acknowledgementSetManager;

    // Should there be a duration or time that is configured in the source to stop processing? Otherwise will only stop when data prepper is stopped
    private volatile boolean isStopped = false;
    private final boolean deleteS3ObjectsOnRead;
    private final S3ObjectDeleteWorker s3ObjectDeleteWorker;
    private final PluginMetrics pluginMetrics;
    private final Counter acknowledgementSetCallbackCounter;

    private final Counter folderPartitionNoObjectsFound;

    private final Counter partitionOwnershipUpdateFailures;
    private final long backOffMs;
    private final List<String> partitionKeys;

    private final FolderPartitioningOptions folderPartitioningOptions;

    private final Map<String, Set<DeleteObjectRequest>> objectsToDeleteForAcknowledgmentSets;

    private final Map<String, AtomicInteger> acknowledgmentsRemainingForPartitions;
    private final Map<String, S3DataSelection> bucketDataSelectionMap;

    private final Duration acknowledgmentSetTimeout;

    public ScanObjectWorker(final S3Client s3Client,
                            final List<ScanOptions> scanOptionsBuilderList,
                            final S3ObjectHandler s3ObjectHandler,
                            final BucketOwnerProvider bucketOwnerProvider,
                            final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                            final S3SourceConfig s3SourceConfig,
                            final AcknowledgementSetManager acknowledgementSetManager,
                            final S3ObjectDeleteWorker s3ObjectDeleteWorker,
                            final long backOffMs,
                            final PluginMetrics pluginMetrics){
        this.s3Client = s3Client;
        this.backOffMs = backOffMs;
        this.scanOptionsBuilderList = scanOptionsBuilderList;
        for (ScanOptions scanOptions : scanOptionsBuilderList) {
        }
        this.s3ObjectHandler= s3ObjectHandler;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.sourceCoordinator = sourceCoordinator;
        this.s3ScanSchedulingOptions = s3SourceConfig.getS3ScanScanOptions().getSchedulingOptions();
        this.bucketDataSelectionMap = new HashMap<>();
        for (S3ScanBucketOptions bucketOption: s3SourceConfig.getS3ScanScanOptions().getBuckets()) {
            if (bucketOption.getS3ScanBucketOption() != null) {
                bucketDataSelectionMap.put(bucketOption.getS3ScanBucketOption().getName(), bucketOption.getS3ScanBucketOption().getDataSelection());
            }
        }
        this.endToEndAcknowledgementsEnabled = s3SourceConfig.getAcknowledgements();
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.deleteS3ObjectsOnRead = s3SourceConfig.isDeleteS3ObjectsOnRead();
        this.s3ObjectDeleteWorker = s3ObjectDeleteWorker;
        this.pluginMetrics = pluginMetrics;
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME);
        this.folderPartitionNoObjectsFound = pluginMetrics.counter(NO_OBJECTS_FOUND_FOR_FOLDER_PARTITION);
        this.partitionOwnershipUpdateFailures = pluginMetrics.counter(PARTITION_OWNERSHIP_UPDATE_ERRORS);
        this.sourceCoordinator.initialize();
        this.partitionKeys = new ArrayList<>();
        this.folderPartitioningOptions = s3SourceConfig.getS3ScanScanOptions().getPartitioningOptions();
        this.acknowledgmentSetTimeout = s3SourceConfig.getS3ScanScanOptions().getAcknowledgmentTimeout();

        this.partitionCreationSupplier = new S3ScanPartitionCreationSupplier(s3Client, bucketOwnerProvider, scanOptionsBuilderList, s3ScanSchedulingOptions, s3SourceConfig.getS3ScanScanOptions().getPartitioningOptions(), s3SourceConfig.isDeleteS3ObjectsOnRead());
        this.acknowledgmentsRemainingForPartitions = new ConcurrentHashMap<>();
        this.objectsToDeleteForAcknowledgmentSets = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        while (!isStopped) {
            try {
                if (System.getProperty(STOP_S3_SCAN_PROCESSING_PROPERTY) == null) {
                    startProcessingObject(backOffMs);
                } else {
                    LOG.debug("System property {} is set, S3 scan is not processing objects", STOP_S3_SCAN_PROCESSING_PROPERTY);
                    try {
                        Thread.sleep(RETRY_BACKOFF_ON_EXCEPTION_MILLIS);
                    } catch (final InterruptedException ex) {
                        LOG.info("S3 Scan worker thread interrupted while waiting for property {} to be set .", STOP_S3_SCAN_PROCESSING_PROPERTY);
                    }
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while processing S3 objects, backing off and retrying", e);
                try {
                    Thread.sleep(RETRY_BACKOFF_ON_EXCEPTION_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.error("S3 Scan worker thread interrupted while backing off.", ex);
                }
            }

        }
        for (String partitionKey: partitionKeys) {
            LOG.debug("Scan object worker is stopped, giving up partitions.");
            sourceCoordinator.giveUpPartition(partitionKey);
        }
    }

    /**
     * For testing
     */
    void runWithoutInfiniteLoop() {
        startProcessingObject(10);
    }

    private void startProcessingObject(final long waitTimeMillis) {
        final Optional<SourcePartition<S3SourceProgressState>> objectToProcess = sourceCoordinator.getNextPartition(partitionCreationSupplier, folderPartitioningOptions != null);
        if (objectToProcess.isEmpty()) {
            try {
                Thread.sleep(waitTimeMillis);
            } catch (InterruptedException e) {
                LOG.error("S3 Scan worker thread interrupted while backing off.", e);
            }
            return;
        }

        partitionKeys.add(objectToProcess.get().getPartitionKey());
        if (folderPartitioningOptions != null) {
            try {
                processFolderPartition(objectToProcess.get());
            } catch (final Exception e) {
                LOG.error("An exception occurred while processing folder partition {}, giving up this partition", objectToProcess.get().getPartitionKey(), e);
                sourceCoordinator.giveUpPartition(objectToProcess.get().getPartitionKey(), Instant.now());
                partitionKeys.remove(objectToProcess.get().getPartitionKey());
            }
            return;
        }

        final String bucket = objectToProcess.get().getPartitionKey().split("\\|")[0];
        final String objectKey = objectToProcess.get().getPartitionKey().split("\\|")[1];

        try {
            AcknowledgementSet acknowledgementSet = null;

            if (endToEndAcknowledgementsEnabled) {
                acknowledgementSet = acknowledgementSetManager.create((result) -> {
                    acknowledgementSetCallbackCounter.increment();
                    // Delete only if this is positive acknowledgement
                    if (result == true) {
                        sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey(), true);
                        final Set<DeleteObjectRequest> deleteObjectsForPartition = objectsToDeleteForAcknowledgmentSets.get(objectToProcess.get().getPartitionKey());
                        deleteObjectsForPartition.forEach(s3ObjectDeleteWorker::deleteS3Object);
                        objectsToDeleteForAcknowledgmentSets.remove(objectToProcess.get().getPartitionKey());
                    } else {
                        LOG.debug("Did not receive positive acknowledgement, giving up partition.");
                        sourceCoordinator.giveUpPartition(objectToProcess.get().getPartitionKey());
                    }
                    partitionKeys.remove(objectToProcess.get().getPartitionKey());
                }, acknowledgmentSetTimeout);

                addProgressCheck(acknowledgementSet, objectToProcess.get());
            }


            final Optional<DeleteObjectRequest> deleteObjectRequest = processS3Object(S3ObjectReference.bucketAndKey(bucket, objectKey).build(),
                    acknowledgementSet, sourceCoordinator, objectToProcess.get());

            if (endToEndAcknowledgementsEnabled) {
                deleteObjectRequest.ifPresent(deleteRequest -> objectsToDeleteForAcknowledgmentSets.put(objectToProcess.get().getPartitionKey(), Set.of(deleteRequest)));
                try {
                    sourceCoordinator.updatePartitionForAcknowledgmentWait(objectToProcess.get().getPartitionKey(), acknowledgmentSetTimeout);
                } catch (final PartitionUpdateException e) {
                    LOG.debug("Failed to update the partition for the acknowledgment wait.");
                }
                acknowledgementSet.complete();
            } else {
                sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey(), false);
                if (s3ObjectDeleteWorker != null) {
                    deleteObjectRequest.ifPresent(s3ObjectDeleteWorker::deleteS3Object);
                }
                partitionKeys.remove(objectToProcess.get().getPartitionKey());
            }
        } catch (final NoSuchKeyException e) {
            LOG.warn("Object {} from bucket {} could not be found, marking this object as complete and continuing processing", objectKey, bucket);
            sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey(), false);
        } catch (final PartitionNotOwnedException | PartitionNotFoundException | PartitionUpdateException e) {
            LOG.warn("S3 scan object worker received an exception from the source coordinator. There is a potential for duplicate data from {}, giving up partition and getting next partition: {}", objectKey, e.getMessage());
            sourceCoordinator.giveUpPartition(objectToProcess.get().getPartitionKey());
        }
    }

    private Optional<DeleteObjectRequest> processS3Object(final S3ObjectReference s3ObjectReference,
                                                          final AcknowledgementSet acknowledgementSet,
                                                          final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                                                          final SourcePartition<S3SourceProgressState> sourcePartition) {
        try {
            S3DataSelection dataSelection = bucketDataSelectionMap.get(s3ObjectReference.getBucketName());
            if (dataSelection == null) {
                dataSelection = S3DataSelection.DATA_AND_METADATA;
            }
            s3ObjectHandler.processS3Object(s3ObjectReference, dataSelection, acknowledgementSet, sourceCoordinator, sourcePartition.getPartitionKey());
            if (deleteS3ObjectsOnRead && endToEndAcknowledgementsEnabled && s3ObjectDeleteWorker != null) {
                final DeleteObjectRequest deleteObjectRequest = s3ObjectDeleteWorker.buildDeleteObjectRequest(s3ObjectReference.getBucketName(), s3ObjectReference.getKey());
                return Optional.of(deleteObjectRequest);
            }
        } catch (final IOException ex) {
            LOG.error("Error while process the processS3Object. ",ex);
        }
        return Optional.empty();
    }

    private void processFolderPartition(final SourcePartition<S3SourceProgressState> folderPartition) {
        final String bucket = folderPartition.getPartitionKey().split("\\|")[0];
        final String s3Prefix = folderPartition.getPartitionKey().split("\\|")[1];

        final List<S3ObjectReference> objectsToProcess = getObjectsForPrefix(bucket, s3Prefix);

        Optional<S3SourceProgressState> folderPartitionState = folderPartition.getPartitionState();

        if (folderPartitionState.isEmpty()) {
            folderPartitionState = Optional.of(new S3SourceProgressState(Instant.now().toEpochMilli()));
            sourceCoordinator.saveProgressStateForPartition(folderPartition.getPartitionKey(), folderPartitionState.get());
        }

        if (objectsToProcess.isEmpty()) {
            folderPartitionNoObjectsFound.increment();
            partitionKeys.remove(folderPartition.getPartitionKey());
            if (shouldDeleteFolderPartition(folderPartition)) {
                LOG.info("Deleting folder partition {} as no objects have been found from this folder for {} minutes", folderPartition.getPartitionKey(), NO_OBJECTS_FOUND_BEFORE_PARTITION_DELETION_DURATION.toMinutes());
                sourceCoordinator.deletePartition(folderPartition.getPartitionKey());
                return;
            }
            LOG.debug("No objects to process, giving up partition");
            sourceCoordinator.giveUpPartition(folderPartition.getPartitionKey(), Instant.now());
            return;
        }

        // Update the last time objects were found to support deletion of the partition after no objects found for some time
        folderPartitionState.ifPresent(state -> state.setLastTimeObjectsFound(Instant.now().toEpochMilli()));
        sourceCoordinator.saveProgressStateForPartition(folderPartition.getPartitionKey(), folderPartitionState.get());

        processObjectsForFolderPartition(objectsToProcess, folderPartition);
    }

    private List<S3ObjectReference> getObjectsForPrefix(final String bucket, final String s3Prefix) {
        ListObjectsV2Response listObjectsV2Response = null;
        final List<S3ObjectReference> objectsToProcess = new ArrayList<>();

        final ListObjectsV2Request.Builder listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(s3Prefix);

        do {
            listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request
                    .fetchOwner(true)
                    .continuationToken(Objects.nonNull(listObjectsV2Response) ? listObjectsV2Response.nextContinuationToken() : null)
                    .build());
            LOG.debug("Found page of {} objects from bucket {} and prefix {}", listObjectsV2Response.keyCount(), bucket, s3Prefix);

            objectsToProcess.addAll(listObjectsV2Response.contents().stream()
                    .map(s3Object -> S3ObjectReference.bucketAndKey(bucket, s3Object.key()).build())
                    .collect(Collectors.toList()));

        } while (listObjectsV2Response.isTruncated());

        return objectsToProcess;
    }

    void stop() {
        isStopped = true;
        Thread.currentThread().interrupt();
    }

    private boolean shouldDeleteFolderPartition(final SourcePartition<S3SourceProgressState> folderPartition) {
        if (folderPartition.getPartitionState().isPresent() &&
                Instant.now().toEpochMilli() - folderPartition.getPartitionState().get().getLastTimeObjectsFound()
                        > NO_OBJECTS_FOUND_BEFORE_PARTITION_DELETION_DURATION.toMillis()) {
            return true;
        }

        return false;
    }


    private void processObjectsForFolderPartition(final List<S3ObjectReference> objectsToProcess,
                                                  final SourcePartition<S3SourceProgressState> folderPartition) {
        int objectsProcessed = 0;
        int objectIndex = 0;
        String activeAcknowledgmentSetId = null;
        AcknowledgementSet acknowledgementSet = null;

        while (objectIndex < objectsToProcess.size() && objectsProcessed < folderPartitioningOptions.getMaxObjectsPerOwnership()) {
            final S3ObjectReference s3ObjectReference = objectsToProcess.get(objectIndex);
            if (objectsProcessed % MAX_OBJECTS_PER_ACKNOWLEDGMENT_SET == 0) {
                if (acknowledgementSet != null) {
                    acknowledgementSet.complete();
                }

                final String acknowledgmentSetId = UUID.randomUUID().toString();
                activeAcknowledgmentSetId = acknowledgmentSetId;

                acknowledgementSet = createAcknowledgmentSetForFolderPartition(folderPartition, acknowledgmentSetId);
                addProgressCheck(acknowledgementSet, folderPartition);

                objectsToDeleteForAcknowledgmentSets.put(acknowledgmentSetId, new HashSet<>());

                final AtomicInteger acknowledgmentsRemainingForPartition = acknowledgmentsRemainingForPartitions.containsKey(folderPartition.getPartitionKey()) ?
                        acknowledgmentsRemainingForPartitions.get(folderPartition.getPartitionKey()) :
                        new AtomicInteger();

                acknowledgmentsRemainingForPartition.incrementAndGet();

                acknowledgmentsRemainingForPartitions.put(folderPartition.getPartitionKey(), acknowledgmentsRemainingForPartition);
            }

            final Optional<DeleteObjectRequest> deleteObjectRequest = processS3Object(s3ObjectReference,
                    acknowledgementSet, sourceCoordinator, folderPartition);

            if (deleteObjectRequest.isPresent()) {
                objectsToDeleteForAcknowledgmentSets.get(activeAcknowledgmentSetId).add(deleteObjectRequest.get());
            }

            objectsProcessed++;
            objectIndex++;
        }

        sourceCoordinator.updatePartitionForAcknowledgmentWait(folderPartition.getPartitionKey(), acknowledgmentSetTimeout);

        if (acknowledgementSet != null) {
            acknowledgementSet.complete();
        }
    }

    private AcknowledgementSet createAcknowledgmentSetForFolderPartition(final SourcePartition<S3SourceProgressState> folderPartition,
                                                                         final String acknowledgmentSetId) {
        return acknowledgementSetManager.create((result) -> {
            acknowledgementSetCallbackCounter.increment();

            // Delete only if this is positive acknowledgement
            if (result) {
                final Set<DeleteObjectRequest> deleteObjectsForPartition = objectsToDeleteForAcknowledgmentSets.get(acknowledgmentSetId);
                deleteObjectsForPartition.forEach(s3ObjectDeleteWorker::deleteS3Object);
            }

            acknowledgmentsRemainingForPartitions.get(folderPartition.getPartitionKey()).decrementAndGet();

            if (acknowledgmentsRemainingForPartitions.get(folderPartition.getPartitionKey()).intValue() == 0) {
                acknowledgmentsRemainingForPartitions.remove(folderPartition.getPartitionKey());
                objectsToDeleteForAcknowledgmentSets.remove(acknowledgmentSetId);
                partitionKeys.remove(folderPartition.getPartitionKey());
                LOG.info("Received all acknowledgments for folder partition {}, giving up this partition", folderPartition.getPartitionKey());
                sourceCoordinator.giveUpPartition(folderPartition.getPartitionKey(), Instant.now());
            }
        }, acknowledgmentSetTimeout);
    }

    private void addProgressCheck(final AcknowledgementSet acknowledgementSet, final SourcePartition<S3SourceProgressState> objectToProcess) {
        acknowledgementSet.addProgressCheck(
                (ratio) -> {
                    try {
                        sourceCoordinator.renewPartitionOwnership(objectToProcess.getPartitionKey());
                    } catch (final PartitionUpdateException | PartitionNotOwnedException | PartitionNotFoundException e) {
                        LOG.debug("Failed to update partition ownership for {} in the acknowledgment progress check", objectToProcess.getPartitionKey());
                        partitionOwnershipUpdateFailures.increment();
                    }
                },
                CHECKPOINT_OWNERSHIP_INTERVAL);
    }
}
