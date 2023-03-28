/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.SourceCoordinator;
import org.opensearch.dataprepper.model.source.SourcePartition;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class S3Service {
    private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);

    private static final String bucketName = "source-coordination-test-bucket";

    private final S3SourceConfig s3SourceConfig;
    private final Buffer<Record<Event>> buffer;
    private final S3Client s3Client;
    private final Codec codec;
    private final CompressionEngine compressionEngine;
    private final PluginMetrics pluginMetrics;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final S3ObjectWorker s3ObjectWorker;
    private final SourceCoordinator sourceCoordinator;
    private final ScheduledExecutorService scheduledExecutorService;

    S3Service(final S3SourceConfig s3SourceConfig,
              final Buffer<Record<Event>> buffer,
              final Codec codec,
              final PluginMetrics pluginMetrics,
              final BucketOwnerProvider bucketOwnerProvider,
              final SourceCoordinator sourceCoordinator) {
        this.s3SourceConfig = s3SourceConfig;
        this.sourceCoordinator = sourceCoordinator;
        this.buffer = buffer;
        this.codec = codec;
        this.pluginMetrics = pluginMetrics;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.s3Client = createS3Client();
        this.compressionEngine = s3SourceConfig.getCompression().getEngine();
        final BiConsumer<Event, S3ObjectReference> eventMetadataModifier = new EventMetadataModifier(s3SourceConfig.getMetadataRootKey());
        this.s3ObjectWorker = new S3ObjectWorker(s3Client, buffer, compressionEngine, codec, bucketOwnerProvider,
                s3SourceConfig.getBufferTimeout(), s3SourceConfig.getNumberOfRecordsToAccumulate(), eventMetadataModifier, pluginMetrics);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::pullObjects, 0, 5, TimeUnit.MINUTES);
        scheduledExecutorService.scheduleAtFixedRate(this::processPartition, 30, 30, TimeUnit.SECONDS);
    }

    void addS3Object(final S3ObjectReference s3ObjectReference) {
        try {
            s3ObjectWorker.parseS3Object(s3ObjectReference);
        } catch (final IOException e) {
            LOG.error("Unable to read S3 object from S3ObjectReference = {}", s3ObjectReference, e);
        }
    }

    private void pullObjects() {
        final List<String> objectKeys = getS3Objects();
        sourceCoordinator.createPartitions(objectKeys);
    }

    private void processPartition() {
        final Optional<SourcePartition> activePartition = sourceCoordinator.getNextPartition();
        activePartition.ifPresent(sourcePartition -> {
            addS3Object(S3ObjectReference.bucketAndKey(bucketName, sourcePartition.getPartitionKey()).build());
            sourceCoordinator.completePartition(sourcePartition.getPartitionKey());
        });
    }

    private List<String> getS3Objects() {
        final ListObjectsResponse response = s3Client.listObjects(ListObjectsRequest.builder()
                .bucket(bucketName)
                .build());

        return response.contents().stream().map(S3Object::key).collect(Collectors.toList());
    }



    S3Client createS3Client() {
        LOG.info("Creating S3 client");
        return S3Client.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }
}
