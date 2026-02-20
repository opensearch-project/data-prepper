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
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants;
import org.opensearch.dataprepper.plugins.source.source_crawler.CrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.DimensionalTimeSliceCrawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourcePlugin;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.metrics.VendorAPIMetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Instant;

import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.PLUGIN_NAME;

@DataPrepperPlugin(name = PLUGIN_NAME,
        pluginType = Source.class,
        pluginConfigurationType = Office365SourceConfig.class,
        packagesToScan = {CrawlerApplicationContextMarker.class, Office365Source.class}
)
public class Office365Source extends CrawlerSourcePlugin {
    private static final Logger LOG = LoggerFactory.getLogger(Office365Source.class);
    private final Office365SourceConfig office365SourceConfig;
    private final Office365AuthenticationInterface office365AuthProvider;
    private final Office365Service office365Service;
    private final VendorAPIMetricsRecorder vendorAPIMetricsRecorder;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    @DataPrepperPluginConstructor
    public Office365Source(final PluginMetrics pluginMetrics,
                           final Office365SourceConfig office365SourceConfig,
                           final Office365AuthenticationInterface office365AuthProvider,
                           final PluginFactory pluginFactory,
                           final AcknowledgementSetManager acknowledgementSetManager,
                           final DimensionalTimeSliceCrawler crawler,
                           final PluginExecutorServiceProvider executorServiceProvider,
                           final Office365Service office365Service,
                           final VendorAPIMetricsRecorder vendorAPIMetricsRecorder) {
        super(PLUGIN_NAME, pluginMetrics, office365SourceConfig, pluginFactory,
                acknowledgementSetManager, crawler, executorServiceProvider);
        crawler.initialize(Arrays.asList(Constants.CONTENT_TYPES));
        LOG.info("Creating Office365 Source Plugin");
        this.office365SourceConfig = office365SourceConfig;
        this.office365AuthProvider = office365AuthProvider;
        this.office365Service = office365Service;
        this.vendorAPIMetricsRecorder = vendorAPIMetricsRecorder;
        
        // Enable subscription metrics at the earliest possible point - plugin initialization
        this.vendorAPIMetricsRecorder.enableSubscriptionMetrics();
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        LOG.info("Starting Office365 Source Plugin...");
        try {
            // Initialize credentials with retry logic - this will not throw exceptions
            // but will retry infinitely
            office365AuthProvider.initCredentials();
            
            // Only proceed with service initialization if credentials are available
            office365Service.initializeSubscriptions();
            super.start(buffer);
        } catch (Exception e) {
            LOG.error("Error starting Office365 Source Plugin", e);
            isRunning.set(false);
            throw new RuntimeException("Failed to start Office365 Source Plugin", e);
        }
    }

    @Override
    protected LeaderProgressState createLeaderProgressState() {
        Instant lastPollTime = Instant.now();
        return new DimensionalTimeSliceLeaderProgressState(lastPollTime, office365SourceConfig.getLookBackDuration(lastPollTime));
    }

    @Override
    public void stop() {
        LOG.info("Stopping Office365 Source Plugin");
        super.stop();
    }
}
