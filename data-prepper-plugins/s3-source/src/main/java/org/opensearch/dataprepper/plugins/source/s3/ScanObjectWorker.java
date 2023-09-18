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
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanSchedulingOptions;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Class responsible for processing the s3 scan objects with the help of <code>S3ObjectWorker</code>
 * or <code>S3SelectWorker</code>.
 */
public class ScanObjectWorker implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(ScanObjectWorker.class);

    private static final int STANDARD_BACKOFF_MILLIS = 30_000;
    private static final int RETRY_BACKOFF_ON_EXCEPTION_MILLIS = 5_000;

    static final int ACKNOWLEDGEMENT_SET_TIMEOUT_SECONDS = Integer.MAX_VALUE;
    static final String ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME = "acknowledgementSetCallbackCounter";

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

    public ScanObjectWorker(final S3Client s3Client,
                            final List<ScanOptions> scanOptionsBuilderList,
                            final S3ObjectHandler s3ObjectHandler,
                            final BucketOwnerProvider bucketOwnerProvider,
                            final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                            final S3SourceConfig s3SourceConfig,
                            final AcknowledgementSetManager acknowledgementSetManager,
                            final S3ObjectDeleteWorker s3ObjectDeleteWorker,
                            final PluginMetrics pluginMetrics){
        this.s3Client = s3Client;
        this.scanOptionsBuilderList = scanOptionsBuilderList;
        this.s3ObjectHandler= s3ObjectHandler;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.sourceCoordinator = sourceCoordinator;
        this.s3ScanSchedulingOptions = s3SourceConfig.getS3ScanScanOptions().getSchedulingOptions();
        this.endToEndAcknowledgementsEnabled = s3SourceConfig.getAcknowledgements();
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.deleteS3ObjectsOnRead = s3SourceConfig.isDeleteS3ObjectsOnRead();
        this.s3ObjectDeleteWorker = s3ObjectDeleteWorker;
        this.pluginMetrics = pluginMetrics;
        acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME);
        this.sourceCoordinator.initialize();

        this.partitionCreationSupplier = new S3ScanPartitionCreationSupplier(s3Client, bucketOwnerProvider, scanOptionsBuilderList, s3ScanSchedulingOptions);
    }

    @Override
    public void run() {
        while (!isStopped) {
            try {
                startProcessingObject(STANDARD_BACKOFF_MILLIS);
            } catch (final Exception e) {
                LOG.error("Received an exception while processing S3 objects, backing off and retrying", e);
                try {
                    Thread.sleep(RETRY_BACKOFF_ON_EXCEPTION_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.error("S3 Scan worker thread interrupted while backing off.", ex);
                    return;
                }
            }

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
                LOG.error("S3 Scan worker thread interrupted while backing off.", e);
            }
            return;
        }

        final String bucket = objectToProcess.get().getPartitionKey().split("\\|")[0];
        final String objectKey = objectToProcess.get().getPartitionKey().split("\\|")[1];

        try {
            List<DeleteObjectRequest> waitingForAcknowledgements = new ArrayList<>();
            AcknowledgementSet acknowledgementSet = null;
            CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();

            if (endToEndAcknowledgementsEnabled) {
                acknowledgementSet = acknowledgementSetManager.create((result) -> {
                    acknowledgementSetCallbackCounter.increment();
                    // Delete only if this is positive acknowledgement
                    if (result == true) {
                        sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey());
                        waitingForAcknowledgements.forEach(s3ObjectDeleteWorker::deleteS3Object);
                    }
                    completableFuture.complete(result);
                }, Duration.ofSeconds(ACKNOWLEDGEMENT_SET_TIMEOUT_SECONDS));
            }


            final Optional<DeleteObjectRequest> deleteObjectRequest = processS3Object(S3ObjectReference.bucketAndKey(bucket, objectKey).build(),
                    acknowledgementSet, sourceCoordinator, objectToProcess.get());

            if (endToEndAcknowledgementsEnabled) {
                deleteObjectRequest.ifPresent(waitingForAcknowledgements::add);
                acknowledgementSet.complete();
                completableFuture.get(ACKNOWLEDGEMENT_SET_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } else {
                sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey());
                deleteObjectRequest.ifPresent(s3ObjectDeleteWorker::deleteS3Object);
            }
        } catch (final NoSuchKeyException e) {
            LOG.warn("Object {} from bucket {} could not be found, marking this object as complete and continuing processing", objectKey, bucket);
            sourceCoordinator.completePartition(objectToProcess.get().getPartitionKey());
        } catch (final PartitionNotOwnedException | PartitionNotFoundException | PartitionUpdateException e) {
            LOG.warn("S3 scan object worker received an exception from the source coordinator. There is a potential for duplicate data from {}, giving up partition and getting next partition: {}", objectKey, e.getMessage());
            sourceCoordinator.giveUpPartitions();
        } catch (final ExecutionException | TimeoutException e) {
            LOG.error("Exception occurred while waiting for acknowledgement.", e);
        } catch (final InterruptedException e) {
            LOG.error("S3 Scan worker thread interrupted while processing S3 object.", e);
        }
    }

    private Optional<DeleteObjectRequest> processS3Object(final S3ObjectReference s3ObjectReference,
                                                          final AcknowledgementSet acknowledgementSet,
                                                          final SourceCoordinator<S3SourceProgressState> sourceCoordinator,
                                                          final SourcePartition<S3SourceProgressState> sourcePartition) {
        try {
            s3ObjectHandler.parseS3Object(s3ObjectReference, acknowledgementSet, sourceCoordinator, sourcePartition.getPartitionKey());
            if (deleteS3ObjectsOnRead && endToEndAcknowledgementsEnabled) {
                final DeleteObjectRequest deleteObjectRequest = s3ObjectDeleteWorker.buildDeleteObjectRequest(s3ObjectReference.getBucketName(), s3ObjectReference.getKey());
                return Optional.of(deleteObjectRequest);
            }
        } catch (final IOException ex) {
            LOG.error("Error while process the parseS3Object. ",ex);
        }
        return Optional.empty();
    }

    void stop() {
        isStopped = true;
        Thread.currentThread().interrupt();
    }
}
