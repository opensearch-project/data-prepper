/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.function.BiConsumer;

public class S3Service {
    private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);

    private final S3SourceConfig s3SourceConfig;
    private final Buffer<Record<Event>> buffer;
    private final S3Client s3Client;
    private final Codec codec;
    private final CompressionEngine compressionEngine;
    private final PluginMetrics pluginMetrics;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final S3ObjectWorker s3ObjectWorker;
    //Issue#1971-Start
    private final S3AsyncClient s3AsyncClient;
    //Issue#1971-Start
    S3Service(final S3SourceConfig s3SourceConfig,
              final Buffer<Record<Event>> buffer,
              final Codec codec,
              final PluginMetrics pluginMetrics,
              final BucketOwnerProvider bucketOwnerProvider) {
        this.s3SourceConfig = s3SourceConfig;
        this.buffer = buffer;
        this.codec = codec;
        this.pluginMetrics = pluginMetrics;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.s3Client = createS3Client();
        this.s3AsyncClient = createS3AsyncClient();
        this.compressionEngine = s3SourceConfig.getCompression().getEngine();
        final BiConsumer<Event, S3ObjectReference> eventMetadataModifier = new EventMetadataModifier(s3SourceConfig.getMetadataRootKey());
        //Issue#1971-Start
        S3SelectObjectWorker s3SelectObjectWorker = new S3SelectObjectWorker(s3AsyncClient, s3SourceConfig, pluginMetrics);
        this.s3ObjectWorker = new S3ObjectWorker(s3Client,s3SourceConfig,s3SelectObjectWorker, buffer, compressionEngine, codec, bucketOwnerProvider,
                s3SourceConfig.getBufferTimeout(), s3SourceConfig.getNumberOfRecordsToAccumulate(), eventMetadataModifier, pluginMetrics);
        //Issue#1971-End
    }

    void addS3Object(final S3ObjectReference s3ObjectReference) {
        try {
            s3ObjectWorker.parseS3Object(s3ObjectReference);
        } catch (final IOException e) {
            LOG.error("Unable to read S3 object from S3ObjectReference = {}", s3ObjectReference, e);
        }
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
    //Issue#1971-Start
    /**
     * Create a S3AsyncClient Object for S3 Select query
     * @return a S3AsyncClient Object
     */
    private S3AsyncClient createS3AsyncClient() {
        LOG.info("Creating S3 Async client");
        return S3AsyncClient.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }
    //Issue#1971-End
}
