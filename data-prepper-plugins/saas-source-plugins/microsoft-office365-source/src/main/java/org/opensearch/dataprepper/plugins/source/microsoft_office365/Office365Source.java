/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.source_crawler.CrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PaginationCrawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourcePlugin;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerLeaderProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Instant;

import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.PLUGIN_NAME;

/**
 * Office 365 Connector main entry point.
 * This class extends CrawlerSourcePlugin to provide Office 365 specific functionality.
 */
@Experimental
@DataPrepperPlugin(name = PLUGIN_NAME,
        pluginType = Source.class,
        pluginConfigurationType = Office365SourceConfig.class,
        packagesToScan = {CrawlerApplicationContextMarker.class, Office365Source.class}
)
public class Office365Source extends CrawlerSourcePlugin {
    private static final Logger LOG = LoggerFactory.getLogger(Office365Source.class);
    private final Office365SourceConfig office365SourceConfig;
    private final Office365AuthenticationInterface office365AuthProvider;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    @DataPrepperPluginConstructor
    public Office365Source(final PluginMetrics pluginMetrics,
                           final Office365SourceConfig office365SourceConfig,
                           final Office365AuthenticationInterface office365AuthProvider,
                           final PluginFactory pluginFactory,
                           final AcknowledgementSetManager acknowledgementSetManager,
                           final PaginationCrawler crawler,
                           final PluginExecutorServiceProvider executorServiceProvider) {
        super(PLUGIN_NAME, pluginMetrics, office365SourceConfig, pluginFactory,
                acknowledgementSetManager, crawler, executorServiceProvider);
        LOG.info("Creating Office365 Source Plugin");
        this.office365SourceConfig = office365SourceConfig;
        this.office365AuthProvider = office365AuthProvider;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        LOG.info("Starting Office365 Source Plugin...");
        try {
            office365AuthProvider.initCredentials();
            super.start(buffer);
        } catch (Exception e) {
            LOG.error("Error starting Office365 Source Plugin", e);
            isRunning.set(false);
            throw new RuntimeException("Failed to start Office365 Source Plugin", e);
        }
    }

    @Override
    protected LeaderProgressState createLeaderProgressState() {
        return new PaginationCrawlerLeaderProgressState(Instant.EPOCH);
    }

    @Override
    public void stop() {
        LOG.info("Stopping Office365 Source Plugin");
        super.stop();
    }
}
