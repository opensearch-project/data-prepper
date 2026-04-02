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

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class PrometheusRemoteWriteSourceConfigTest {

    @Test
    void testDefaultPort() {
        final PrometheusRemoteWriteSourceConfig config = new PrometheusRemoteWriteSourceConfig();
        assertThat(config.getDefaultPort(), equalTo(9090));
    }

    @Test
    void testDefaultPath() {
        final PrometheusRemoteWriteSourceConfig config = new PrometheusRemoteWriteSourceConfig();
        assertThat(config.getDefaultPath(), equalTo("/api/v1/write"));
    }

    @Test
    void testDefaultFlattenLabels() {
        final PrometheusRemoteWriteSourceConfig config = new PrometheusRemoteWriteSourceConfig();
        assertThat(config.isFlattenLabels(), equalTo(false));
    }

    @Test
    void testDefaultRequestTimeout() {
        final PrometheusRemoteWriteSourceConfig config = new PrometheusRemoteWriteSourceConfig();
        assertThat(config.getRequestTimeoutInMillis(), equalTo(10_000));
    }

    @Test
    void testDefaultHealthCheckDisabled() {
        final PrometheusRemoteWriteSourceConfig config = new PrometheusRemoteWriteSourceConfig();
        assertThat(config.hasHealthCheckService(), equalTo(false));
    }
}
