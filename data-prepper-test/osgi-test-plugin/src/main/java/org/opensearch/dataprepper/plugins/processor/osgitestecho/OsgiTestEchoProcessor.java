/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.osgitestecho;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ServiceLoader;

/**
 * A pass-through processor that stamps an SPI-resolved greeting onto each event and returns the
 * records otherwise unchanged. Used exclusively for integration testing the OSGi plugin loading
 * pipeline. This plugin lives in data-prepper-test and is NOT included in the release distribution.
 * <p>
 * The constructor deliberately resolves {@link OsgiTestGreetingProvider} through
 * {@code ServiceLoader.load(Class)}, the idiom that reads the thread context classloader. Under OSGi
 * that only succeeds if the plugin framework scopes the thread context classloader to the plugin's
 * bundle classloader for construction, so the greeting an integration test observes is direct
 * evidence of SPI support at the construction boundary.
 */
@DataPrepperPlugin(name = "osgi_test_echo", pluginType = Processor.class)
public class OsgiTestEchoProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiTestEchoProcessor.class);

    /**
     * The key each handled event is stamped with. Integration tests read it to learn what the
     * constructor's SPI lookup resolved.
     */
    public static final String GREETING_KEY = "osgi_test_spi_greeting";

    /**
     * The value stamped when the SPI lookup found no provider. The processor records this rather than
     * throwing so that a missing thread context classloader shows up as a failed assertion on the
     * greeting instead of as a plugin that cannot be constructed at all.
     */
    public static final String SPI_UNAVAILABLE_GREETING = "spi-lookup-failed";

    private final String greeting;

    @DataPrepperPluginConstructor
    public OsgiTestEchoProcessor(final PluginMetrics pluginMetrics) {
        super(pluginMetrics);
        this.greeting = ServiceLoader.load(OsgiTestGreetingProvider.class)
                .findFirst()
                .map(OsgiTestGreetingProvider::greeting)
                .orElse(SPI_UNAVAILABLE_GREETING);
        LOG.info("osgi_test_echo resolved its greeting provider via SPI: {}", greeting);
    }

    /**
     * @return the greeting resolved by the constructor's {@code ServiceLoader} lookup
     */
    public String getGreeting() {
        return greeting;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            record.getData().put(GREETING_KEY, greeting);
            LOG.debug("osgi_test_echo: {}", record.getData().toJsonString());
        }
        return records;
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
