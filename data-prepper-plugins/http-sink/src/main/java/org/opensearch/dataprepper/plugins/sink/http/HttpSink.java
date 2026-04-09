/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.common.sink.DefaultSinkMetrics;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.http.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.http.service.HttpSinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

@DataPrepperPlugin(name = "http", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HttpSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);

    private volatile boolean sinkInitialized;

    private final HttpSinkService httpSinkService;

    @DataPrepperPluginConstructor
    public HttpSink(final PluginSetting pluginSetting,
                    final HttpSinkConfiguration httpSinkConfiguration,
                    final PluginFactory pluginFactory,
                    final PipelineDescription pipelineDescription,
                    final SinkContext sinkContext,
                    final AwsCredentialsSupplier awsCredentialsSupplier,
                    final PluginMetrics pluginMetrics) {
        super(pluginSetting);
        this.sinkInitialized = false;

        final SinkContext context = sinkContext != null ? sinkContext : new SinkContext(null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        final PluginModel codecConfiguration = httpSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        codecPluginSettings.setPipelineName(pipelineDescription.getPipelineName());

        final OutputCodec codec = pluginFactory.loadPlugin(OutputCodec.class, codecPluginSettings);
        final OutputCodecContext codecContext = OutputCodecContext.fromSinkContext(context);

        final SinkMetrics sinkMetrics = new DefaultSinkMetrics(pluginMetrics, "Event");

        final HttpSinkSender httpSender = new HttpSinkSender(
                    httpSinkConfiguration.getAwsConfig() != null ? awsCredentialsSupplier : null,
                    httpSinkConfiguration,
                    sinkMetrics);

        this.httpSinkService = new HttpSinkService(
                httpSinkConfiguration,
                sinkMetrics,
                httpSender,
                pipelineDescription,
                codec,
                codecContext);
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        sinkInitialized = true;
        httpSinkService.setDlqPipeline(getFailurePipeline());
    }

    @VisibleForTesting
    void setDlqPipeline(HeadlessPipeline dlqPipeline) { httpSinkService.setDlqPipeline(dlqPipeline); }

    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        httpSinkService.output(records);
    }
}
