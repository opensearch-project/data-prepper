/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.rss.config.FeedSourceConfig;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class FeedSourceTest {

    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("rss", "test");

    @Test
    void start_null_buffer_throws() {
        final FeedSource source = new FeedSource(pluginMetrics, mock(FeedSourceConfig.class));
        assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    void stop_without_start_is_safe() {
        final FeedSource source = new FeedSource(pluginMetrics, mock(FeedSourceConfig.class));
        source.stop(); // must not throw when executor was never created
    }

    @Test
    void worker_count_is_bounded_by_feeds_and_at_least_one() {
        assertThat(FeedSource.workerCount(5, 2), equalTo(2));
        assertThat(FeedSource.workerCount(1, 4), equalTo(1));
        assertThat(FeedSource.workerCount(3, 10), equalTo(3));
        assertThat(FeedSource.workerCount(0, 0), equalTo(1));
    }
}
