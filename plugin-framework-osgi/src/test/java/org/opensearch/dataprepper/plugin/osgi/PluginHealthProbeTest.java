/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the {@link PluginHealthProbe} interface contract. Since this is a seam
 * interface with no implementation yet, we verify that a test implementation
 * can be created and its methods behave as expected.
 */
class PluginHealthProbeTest {

    @Test
    void implementation_returns_expected_values() {
        final PluginHealthProbe probe = new PluginHealthProbe() {
            @Override
            public String getBundleSymbolicName() {
                return "test.bundle";
            }

            @Override
            public boolean isHealthy() {
                return true;
            }

            @Override
            public String getStatusMessage() {
                return "All good";
            }
        };

        assertThat(probe.getBundleSymbolicName(), is("test.bundle"));
        assertThat(probe.isHealthy(), is(true));
        assertThat(probe.getStatusMessage(), is("All good"));
    }

    @Test
    void unhealthy_probe_reports_correctly() {
        final PluginHealthProbe probe = new PluginHealthProbe() {
            @Override
            public String getBundleSymbolicName() {
                return "failing.bundle";
            }

            @Override
            public boolean isHealthy() {
                return false;
            }

            @Override
            public String getStatusMessage() {
                return "Internal resource unavailable";
            }
        };

        assertThat(probe.getBundleSymbolicName(), is("failing.bundle"));
        assertThat(probe.isHealthy(), is(false));
        assertThat(probe.getStatusMessage(), is("Internal resource unavailable"));
    }
}
