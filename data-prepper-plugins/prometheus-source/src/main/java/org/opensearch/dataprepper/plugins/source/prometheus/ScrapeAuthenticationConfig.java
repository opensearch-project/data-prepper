/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import org.opensearch.dataprepper.plugins.http.client.auth.HttpClientAuthenticationConfig;

/**
 * Authentication configuration for the Prometheus scrape source.
 * Extends the shared {@link HttpClientAuthenticationConfig} to inherit
 * HTTP Basic and Bearer Token authentication support.
 */
public class ScrapeAuthenticationConfig extends HttpClientAuthenticationConfig {
}
