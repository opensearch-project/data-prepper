/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.UrlConfigurationOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@DataPrepperPlugin(name = "http", pluginType = Sink.class, pluginConfigurationType = HttpSinkConfiguration.class)
public class HTTPSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(HTTPSink.class);

    private final HttpSinkConfiguration httpSinkConfiguration;

    private volatile boolean sinkInitialized;

    @DataPrepperPluginConstructor
    public HTTPSink(final PluginSetting pluginSetting,
                    final HttpSinkConfiguration httpSinkConfiguration,
                    final PluginFactory pluginFactory,
                    final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginSetting);
        this.httpSinkConfiguration = httpSinkConfiguration;
        sinkInitialized = Boolean.FALSE;
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
        //TODO:  call Service call method
    }


    public Optional<CloseableHttpClient> getAuthHandlerByConfig(final HttpSinkConfiguration sinkConfiguration){
        //TODO: AWS Sigv4 - check
        // TODO: call Auth Handlers based on auth Type

        return null;
    }

    public List<HttpAuthOptions> getClassicHttpRequestList(final List<UrlConfigurationOption> urlConfigurationOption){
        // logic for create auth handler for each url based on provided configuration - getAuthHandlerByConfig()
        // logic for request preparation for each url
        // logic for worker is not there in url level then verify the global workers if global workers also not defined then default 1
        // logic for get the Proxy object if url level proxy enabled else look the global proxy.
        // Aws SageMaker headers if headers found in the configuration
        return null;
    }
}