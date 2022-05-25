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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import java.util.Objects;

@DataPrepperPlugin(name = "s3", pluginType = Source.class, pluginConfigurationType = S3SourceConfig.class)
public class S3Source implements Source<Record<Event>> {

    private final PluginMetrics pluginMetrics;
    private final S3SourceConfig s3SourceConfig;

    @DataPrepperPluginConstructor
    public S3Source(PluginMetrics pluginMetrics, final S3SourceConfig s3SourceConfig) {
        this.pluginMetrics = pluginMetrics;
        this.s3SourceConfig = s3SourceConfig;

        awsCredentialsProvider = createCredentialsProvider();
        s3Client = createS3Client(awsCredentialsProvider);

        throw new NotImplementedException();
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {

    }

    @Override
    public void stop() {

    }

    private final AwsCredentialsProvider awsCredentialsProvider;
    private final S3Client s3Client;

    private AwsCredentialsProvider createCredentialsProvider() {
        return Objects.requireNonNull(s3SourceConfig.getAWSAuthentication().authenticateAwsConfiguration(StsClient.create()));
    }

    private S3Client createS3Client(final AwsCredentialsProvider awsCredentialsProvider) {
        return S3Client.builder()
                .region(Region.of(s3SourceConfig.getAWSAuthentication().getAwsRegion()))
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }
}
