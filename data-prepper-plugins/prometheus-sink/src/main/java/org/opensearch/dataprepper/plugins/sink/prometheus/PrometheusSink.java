/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus;

import com.google.common.annotations.VisibleForTesting;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.aws.api.AwsConfig;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkThresholdConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.common.sink.DefaultSinkMetrics;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.service.PrometheusSinkService;
import software.amazon.awssdk.regions.Region;

import java.util.Collection;

@DataPrepperPlugin(name = "prometheus", pluginType = Sink.class, pluginConfigurationType = PrometheusSinkConfiguration.class)
public class PrometheusSink extends AbstractSink<Record<Event>> {

    private volatile boolean sinkInitialized;
    private final PrometheusSinkService prometheusSinkService;
    private PrometheusHttpSender httpSender;
    private SinkMetrics sinkMetrics;

    @DataPrepperPluginConstructor
    public PrometheusSink(final PluginSetting pluginSetting,
                    final PluginMetrics pluginMetrics,
                    final PluginFactory pluginFactory,
                    final PrometheusSinkConfiguration prometheusSinkConfiguration,
                    final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        this.sinkInitialized = Boolean.FALSE;
        AwsConfig awsConfig = prometheusSinkConfiguration.getAwsConfig();
        final AwsCredentialsProvider awsCredentialsProvider = (awsConfig != null) ? awsCredentialsSupplier.getProvider(convertToCredentialOptions(awsConfig)) : awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder().build());
        Region region = (awsConfig != null) ? awsConfig.getAwsRegion() : awsCredentialsSupplier.getDefaultRegion().get();
      
        sinkMetrics = new DefaultSinkMetrics(pluginMetrics, "metric");
        httpSender = new PrometheusHttpSender(awsCredentialsSupplier, prometheusSinkConfiguration, sinkMetrics, prometheusSinkConfiguration.getConnectionTimeoutMs(), prometheusSinkConfiguration.getIdleTimeoutMs());

        PrometheusSinkThresholdConfig thresholdConfig = prometheusSinkConfiguration.getThresholdConfig();

        this.prometheusSinkService = new PrometheusSinkService(
                prometheusSinkConfiguration,
                sinkMetrics,
                httpSender,
                getFailurePipeline(),
                pluginMetrics,
                pluginSetting);
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
        sinkInitialized = Boolean.TRUE;
    }

    @VisibleForTesting
    void setDlqPipeline(HeadlessPipeline dlqPipeline) {
        prometheusSinkService.setDlqPipeline(dlqPipeline);
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        prometheusSinkService.output(records);
    }
}
