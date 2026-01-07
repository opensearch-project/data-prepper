/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.configuration;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365RestClient;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.metrics.VendorAPIMetricsRecorder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for Microsoft Office 365 components.
 * 
 * This configuration class creates all Office 365 related beans with their dependencies:
 * 1. VendorAPIMetricsRecorder for unified metrics across all operations
 * 2. Office365AuthenticationProvider with authentication metrics support
 * 3. Office365RestClient with VendorAPIMetricsRecorder
 * 
 * This provides comprehensive metrics coverage for authentication, API calls, 
 * and subscription management operations.
 */
@Configuration
public class Office365Configuration {

    /**
     * Creates VendorAPIMetricsRecorder with unified metrics for all operations.
     * 
     * @param pluginMetrics The system plugin metrics instance
     * @return Configured VendorAPIMetricsRecorder
     */
    @Bean
    public VendorAPIMetricsRecorder vendorAPIMetricsRecorder(PluginMetrics pluginMetrics) {
        return new VendorAPIMetricsRecorder(pluginMetrics);
    }

    /**
     * Creates Office365AuthenticationProvider with metrics recording capabilities.
     *
     * @param config The Office 365 source configuration
     * @param metricsRecorder The metrics recorder for authentication operations
     * @return Configured Office365AuthenticationProvider with metrics support
     */
    @Bean
    public Office365AuthenticationProvider office365AuthenticationProvider(
            Office365SourceConfig config,
            VendorAPIMetricsRecorder metricsRecorder) {
        return new Office365AuthenticationProvider(config, metricsRecorder);
    }


    /**
     * Creates Office365RestClient with unified metrics recorder.
     * 
     * @param authConfig The Office 365 authentication provider
     * @param vendorAPIMetricsRecorder The unified metrics recorder
     * @return Configured Office365RestClient
     */
    @Bean
    public Office365RestClient office365RestClient(
            Office365AuthenticationInterface authConfig,
            VendorAPIMetricsRecorder vendorAPIMetricsRecorder) {
        return new Office365RestClient(authConfig, vendorAPIMetricsRecorder);
    }
}
