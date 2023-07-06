/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.client5.http.HttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.util.TimeValue;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.LocalFileBufferFactory;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.dlq.HttpSinkDlqUtil;
import org.opensearch.dataprepper.plugins.sink.service.HttpSinkService;
import org.opensearch.dataprepper.plugins.sink.service.WebhookService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;

@DataPrepperPlugin(name = "http", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HTTPSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);

    private WebhookService webhookService;

    private volatile boolean sinkInitialized;

    private final HttpSinkService httpSinkService;

    private final BufferFactory bufferFactory;

    private HttpSinkDlqUtil httpSinkDLQService;

    @DataPrepperPluginConstructor
    public HTTPSink(final PluginSetting pluginSetting,
                    final HttpSinkConfiguration httpSinkConfiguration,
                    final PluginFactory pluginFactory,
                    final PipelineDescription pipelineDescription) {
        super(pluginSetting);
        final PluginModel codecConfiguration = httpSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        codecPluginSettings.setPipelineName(pipelineDescription.getPipelineName());
        this.sinkInitialized = Boolean.FALSE;
        if (httpSinkConfiguration.getBufferType().equals(BufferTypeOptions.LOCALFILE)) {
            this.bufferFactory = new LocalFileBufferFactory();
        } else {
            this.bufferFactory = new InMemoryBufferFactory();
        }

        if(Objects.nonNull(httpSinkConfiguration.getDlq()))
            this.httpSinkDLQService = new HttpSinkDlqUtil(pluginFactory,httpSinkConfiguration);

        final HttpRequestRetryStrategy httpRequestRetryStrategy = new DefaultHttpRequestRetryStrategy(httpSinkConfiguration.getMaxUploadRetries(),
                TimeValue.of(httpSinkConfiguration.getHttpRetryInterval()));

        final HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRetryStrategy(httpRequestRetryStrategy);

        if(Objects.nonNull(httpSinkConfiguration.getWebhookURL()))
            this.webhookService = new WebhookService(httpSinkConfiguration.getWebhookURL(),httpClientBuilder);

        this.httpSinkService = new HttpSinkService(
                httpSinkConfiguration,
                bufferFactory,
                httpSinkDLQService,
                codecPluginSettings,
                webhookService,
                httpClientBuilder,
                pluginMetrics);
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize http-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize http-sink plugin.");
            this.shutdown();
            throw e;
        }
    }

    private void doInitializeInternal() {
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }
        httpSinkService.output(records);
    }
}