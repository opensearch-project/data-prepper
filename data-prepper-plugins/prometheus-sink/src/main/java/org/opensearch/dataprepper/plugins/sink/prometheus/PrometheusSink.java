 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */

package org.opensearch.dataprepper.plugins.sink.prometheus;

import com.google.common.annotations.VisibleForTesting;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.common.sink.SinkMetrics;
import org.opensearch.dataprepper.common.sink.DefaultSinkMetrics;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.service.PrometheusSinkService;

import java.util.Collection;

@Experimental
@DataPrepperPlugin(name = "prometheus", pluginType = Sink.class, pluginConfigurationType = PrometheusSinkConfiguration.class)
public class PrometheusSink extends AbstractSink<Record<Event>> {

    private volatile boolean sinkInitialized;
    private final PrometheusSinkService prometheusSinkService;
    private final PrometheusHttpSender httpSender;
    private final SinkMetrics sinkMetrics;

    @DataPrepperPluginConstructor
    public PrometheusSink(final PluginSetting pluginSetting,
                    final PluginMetrics pluginMetrics,
                    final PipelineDescription pipelineDescription,
                    final PrometheusSinkConfiguration prometheusSinkConfiguration,
                    final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        this.sinkInitialized = false;

        sinkMetrics = new DefaultSinkMetrics(pluginMetrics, "Metric");
        httpSender = new PrometheusHttpSender(awsCredentialsSupplier, prometheusSinkConfiguration, sinkMetrics);

        this.prometheusSinkService = new PrometheusSinkService(
                prometheusSinkConfiguration,
                sinkMetrics,
                httpSender,
                pipelineDescription);
    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        sinkInitialized = true;
        prometheusSinkService.setDlqPipeline(getFailurePipeline());
    }

    @VisibleForTesting
    void setDlqPipeline(final HeadlessPipeline dlqPipeline) {
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
