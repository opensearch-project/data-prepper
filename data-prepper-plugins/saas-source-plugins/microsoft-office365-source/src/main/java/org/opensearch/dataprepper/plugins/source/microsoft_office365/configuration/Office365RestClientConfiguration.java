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
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.source_crawler.metrics.VendorAPIMetricsRecorder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for Microsoft Office 365 RestClient.
 * 
 * This configuration class creates the Office365RestClient with the required dependencies:
 * 1. Office365AuthenticationInterface for authentication
 * 2. PluginMetrics for unified metrics recording
 * 
 * The Office365RestClient internally creates VendorAPIMetricsRecorder instances 
 * for different operation types (GET, SEARCH, AUTH) from the provided PluginMetrics.
 */
@Configuration
public class Office365RestClientConfiguration {

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
     * Creates Office365RestClient with unified metrics recorder.
     * 
     * @param authConfig The Office 365 authentication provider
     * @param pluginMetrics The system plugin metrics instance
     * @param vendorAPIMetricsRecorder The unified metrics recorder
     * @return Configured Office365RestClient
     */
    @Bean
    public Office365RestClient office365RestClient(
            Office365AuthenticationInterface authConfig,
            PluginMetrics pluginMetrics,
            VendorAPIMetricsRecorder vendorAPIMetricsRecorder) {
        return new Office365RestClient(authConfig, pluginMetrics, vendorAPIMetricsRecorder);
    }
}
