/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.annotations.Experimental;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsClientFactory;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.dlq.DlqPushHandler;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.regions.Region;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;

@Experimental
@DataPrepperPlugin(name = "sqs", pluginType = Sink.class, pluginConfigurationType = SqsSinkConfig.class)
public class SqsSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(SqsSink.class);
    private static final Duration RETRY_FLUSH_BACKOFF = Duration.ofSeconds(5);
    private final SqsSinkConfig sqsSinkConfig;
    private volatile boolean sinkInitialized;
    private final SqsSinkService sqsSinkService;

    @DataPrepperPluginConstructor
    public SqsSink(final PluginSetting pluginSetting,
                   final PluginMetrics pluginMetrics,
                   final PluginFactory pluginFactory,
                   final SqsSinkConfig sqsSinkConfig,
                   final SinkContext sinkContext,
                   final ExpressionEvaluator expressionEvaluator,
                   final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        this.sqsSinkConfig = sqsSinkConfig;
        sinkInitialized = false;
        final PluginModel codecConfiguration = sqsSinkConfig.getCodec();
        final PluginSetting codecPluginSettings;
        String codecPluginName = codecConfiguration.getPluginName();
        if (!codecPluginName.equals("json") && !codecPluginName.equals("ndjson")) {
            throw new RuntimeException(String.format("Codec {} not supported.", codecPluginName));
        }
        codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
            codecConfiguration.getPluginSettings());
        
        AwsConfig awsConfig = sqsSinkConfig.getAwsConfig();
        final AwsCredentialsProvider awsCredentialsProvider = (awsConfig != null) ? awsCredentialsSupplier.getProvider(convertToCredentialOptions(awsConfig)) : awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder().build());
        Region region = (awsConfig != null) ? awsConfig.getAwsRegion() : awsCredentialsSupplier.getDefaultRegion().get();
        final SqsClient sqsClient = SqsClientFactory.createSqsClient(region, awsCredentialsProvider);

        DlqPushHandler dlqPushHandler = null;
        if (sqsSinkConfig.getDlq() != null) {
            StsClient stsClient = StsClient.builder()
                    .region(region)
                    .credentialsProvider(awsCredentialsProvider)
                    .build();
            String role = stsClient.getCallerIdentity().arn();
            dlqPushHandler = new DlqPushHandler(pluginFactory, pluginSetting, pluginMetrics, sqsSinkConfig.getDlq(), region.toString(), role, "sqsSink");
        }
        final OutputCodec outputCodec = pluginFactory.loadPlugin(OutputCodec.class, codecPluginSettings);
        sqsSinkService = new SqsSinkService(sqsSinkConfig, sqsClient, expressionEvaluator, outputCodec, sinkContext, dlqPushHandler, pluginMetrics);
    }

    private static AwsCredentialsOptions convertToCredentialOptions(final AwsConfig awsConfig) {
        return AwsCredentialsOptions.builder()
                .withRegion(awsConfig.getAwsRegion())
                .withStsRoleArn(awsConfig.getAwsStsRoleArn())
                .withStsExternalId(awsConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsConfig.getAwsStsHeaderOverrides())
                .build();
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        sinkInitialized = true;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        sqsSinkService.output(records);
    }
}

