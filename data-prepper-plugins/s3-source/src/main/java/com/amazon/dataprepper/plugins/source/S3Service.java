/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.codec.Codec;
import com.amazon.dataprepper.plugins.source.compression.CompressionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import java.io.IOException;

public class S3Service {
    private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);

    private final S3SourceConfig s3SourceConfig;
    private final Buffer<Record<Event>> buffer;
    private final S3Client s3Client;
    private final Codec codec;
    private final CompressionEngine compressionEngine;
    private final PluginMetrics pluginMetrics;
    private final S3ObjectWorker s3ObjectWorker;

    public S3Service(final S3SourceConfig s3SourceConfig, Buffer<Record<Event>> buffer, Codec codec, PluginMetrics pluginMetrics) {
        this.s3SourceConfig = s3SourceConfig;
        this.buffer = buffer;
        this.codec = codec;
        this.pluginMetrics = pluginMetrics;
        this.s3Client = createS3Client(StsClient.create());
        this.compressionEngine = s3SourceConfig.getCompression().getEngine();
        this.s3ObjectWorker = new S3ObjectWorker(s3Client, buffer, compressionEngine, codec,
                s3SourceConfig.getRequestTimeout(), s3SourceConfig.getNumberOfRecordsToAccumulate(), pluginMetrics);
    }

    void addS3Object(final S3ObjectReference s3ObjectReference) {
        try {
            s3ObjectWorker.parseS3Object(s3ObjectReference);
        } catch (IOException e) {
            LOG.error("Unable to read S3 object from S3ObjectReference = {}", s3ObjectReference, e);
        }
    }

    S3Client createS3Client(final StsClient stsClient) {
        LOG.info("Creating S3 client");
        return S3Client.builder()
                .region(Region.of(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion()))
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration(stsClient))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }
}
