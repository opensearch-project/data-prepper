/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import java.util.Objects;

@DataPrepperPlugin(name = "s3", pluginType = Source.class, pluginConfigurationType = S3SourceConfig.class)
public class S3Source implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(S3Source.class);

    private final PluginMetrics pluginMetrics;
    private final S3SourceConfig s3SourceConfig;
    private AwsCredentialsProvider awsCredentialsProvider;
    private S3Client s3Client;
    private SqsClient sqsClient;

    @DataPrepperPluginConstructor
    public S3Source(PluginMetrics pluginMetrics, final S3SourceConfig s3SourceConfig) {
        this.pluginMetrics = pluginMetrics;
        this.s3SourceConfig = s3SourceConfig;

        awsCredentialsProvider = createCredentialsProvider();
        LOG.info("Creating S3 client.");
        s3Client = createS3Client(awsCredentialsProvider);
        sqsClient = SqsClient.create();

        throw new NotImplementedException();
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        for(int i = 0; i < s3SourceConfig.getSqsOptions().getThreadCount(); i++) {
            Thread sqsWorkerThread = new Thread(new SqsWorker(sqsClient, s3Client, s3SourceConfig));
            sqsWorkerThread.start();
        }
    }

    @Override
    public void stop() {

    }

    public AwsCredentialsProvider createCredentialsProvider() {
        return Objects.requireNonNull(s3SourceConfig.getAWSAuthentication().authenticateAwsConfiguration(StsClient.create()));
    }

    private S3Client createS3Client(final AwsCredentialsProvider awsCredentialsProvider) {
        return S3Client.builder()
                .region(Region.of(s3SourceConfig.getAWSAuthentication().getAwsRegion()))
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }
}
