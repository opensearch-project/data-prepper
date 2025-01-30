/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.confluence.rest.auth.ConfluenceAuthConfig;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceConfigHelper;
import org.opensearch.dataprepper.plugins.source.source_crawler.CrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourcePlugin;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.PLUGIN_NAME;


/**
 * JiraConnector connector entry point.
 */

@DataPrepperPlugin(name = PLUGIN_NAME,
        pluginType = Source.class,
        pluginConfigurationType = ConfluenceSourceConfig.class,
        packagesToScan = {CrawlerApplicationContextMarker.class, ConfluenceSource.class}
)
public class ConfluenceSource extends CrawlerSourcePlugin {

    private static final Logger log = LoggerFactory.getLogger(ConfluenceSource.class);
    private final ConfluenceSourceConfig confluenceSourceConfig;
    private final ConfluenceAuthConfig jiraOauthConfig;

    @DataPrepperPluginConstructor
    public ConfluenceSource(final PluginMetrics pluginMetrics,
                            final ConfluenceSourceConfig confluenceSourceConfig,
                            final ConfluenceAuthConfig jiraOauthConfig,
                            final PluginFactory pluginFactory,
                            final AcknowledgementSetManager acknowledgementSetManager,
                            Crawler crawler,
                            PluginExecutorServiceProvider executorServiceProvider) {
        super(PLUGIN_NAME, pluginMetrics, confluenceSourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
        log.info("Creating Jira Source Plugin");
        this.confluenceSourceConfig = confluenceSourceConfig;
        this.jiraOauthConfig = jiraOauthConfig;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        log.info("Starting Jira Source Plugin... ");
        ConfluenceConfigHelper.validateConfig(confluenceSourceConfig);
        jiraOauthConfig.initCredentials();
        super.start(buffer);
    }

    @Override
    public void stop() {
        log.info("Stopping Jira Source Plugin");
        super.stop();
    }

}
