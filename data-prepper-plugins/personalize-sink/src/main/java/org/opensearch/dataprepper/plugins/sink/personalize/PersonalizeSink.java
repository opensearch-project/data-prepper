/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.personalize;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.PersonalizeSinkConfiguration;
import software.amazon.awssdk.services.personalizeevents.PersonalizeEventsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Implementation class of personalize-sink plugin. It is responsible for receiving the collection of
 * {@link Event} and uploading to amazon personalize.
 */
@DataPrepperPlugin(name = "aws_personalize", pluginType = Sink.class, pluginConfigurationType = PersonalizeSinkConfiguration.class)
public class PersonalizeSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(PersonalizeSink.class);

    private final PersonalizeSinkConfiguration personalizeSinkConfig;
    private volatile boolean sinkInitialized;
    private final PersonalizeSinkService personalizeSinkService;
    private final SinkContext sinkContext;

    /**
     * @param pluginSetting dp plugin settings.
     * @param personalizeSinkConfig personalize sink configurations.
     * @param sinkContext sink context
     * @param awsCredentialsSupplier aws credentials
     * @param pluginFactory dp plugin factory.
     */
    @DataPrepperPluginConstructor
    public PersonalizeSink(final PluginSetting pluginSetting,
                           final PersonalizeSinkConfiguration personalizeSinkConfig,
                           final PluginFactory pluginFactory,
                           final SinkContext sinkContext,
                           final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        this.personalizeSinkConfig = personalizeSinkConfig;
        this.sinkContext = sinkContext;

        sinkInitialized = false;

        final PersonalizeEventsClient personalizeEventsClient = ClientFactory.createPersonalizeEventsClient(personalizeSinkConfig, awsCredentialsSupplier);

        personalizeSinkService = new PersonalizeSinkService(personalizeSinkConfig, pluginMetrics);
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
        personalizeSinkService.output(records);
    }
}