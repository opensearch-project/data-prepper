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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

@DataPrepperPlugin(name = "s3", pluginType = Source.class, pluginConfigurationType = S3SourceConfig.class)
public class S3Source implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(S3Source.class);

    private final PluginMetrics pluginMetrics;
    private final S3SourceConfig s3SourceConfig;
    private software.amazon.awssdk.services.s3.S3Client s3Client;
    private SqsClient sqsClient;
    private Thread sqsWorkerThread;

    @DataPrepperPluginConstructor
    public S3Source(PluginMetrics pluginMetrics, final S3SourceConfig s3SourceConfig) {
        this.pluginMetrics = pluginMetrics;
        this.s3SourceConfig = s3SourceConfig;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        LOG.info("Creating SQS and S3 client");
        StsClient stsClient = StsClient.create();
        this.s3Client = new S3ClientAuthentication(s3SourceConfig).createS3Client(stsClient);
        this.sqsClient = new SqsClientAuthentication(s3SourceConfig).createSqsClient(stsClient);

        sqsWorkerThread = new Thread(new SqsWorker(sqsClient, s3Client, s3SourceConfig));
    }

    @Override
    public void stop() {
        Thread.currentThread().interrupt();
    }
}
