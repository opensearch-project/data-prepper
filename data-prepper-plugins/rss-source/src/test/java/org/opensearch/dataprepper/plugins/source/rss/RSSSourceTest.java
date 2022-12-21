/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RSSSourceTest {

    private final String PLUGIN_NAME = "rss";

    private final String PIPELINE_NAME = "test";

    private final String VALID_RSS_URL = "https://forum.opensearch.org/latest.rss";

    private final String INVALID_RSS_URL = "https://docs.aws.amazon.com/amazondynamodb/latest";

    private PluginMetrics pluginMetrics;

    private RSSSourceConfig rssSourceConfig;

    private RSSSource rssSource;

    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, PIPELINE_NAME);
        rssSourceConfig = mock(RSSSourceConfig.class);
        rssSource = mock(RSSSource.class);
    }

    @Test
    void test_when_extractItemsFromRssFeed_captured() {
        ArgumentCaptor<RSSSourceConfig> argumentCaptor = ArgumentCaptor.forClass(RSSSourceConfig.class);
        when(rssSourceConfig.getUrl()).thenReturn(VALID_RSS_URL);
        doNothing().when(rssSource).extractItemsFromRssFeed(argumentCaptor.capture());
        rssSource.extractItemsFromRssFeed(rssSourceConfig);
        assertEquals(rssSourceConfig, argumentCaptor.getValue());
    }

    @ParameterizedTest
    @ValueSource(strings = { "https://www.yahoo.com/news/rss/mostviewed", "https://forum.opensearch.org/latest.rss",
            "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/dynamodbupdates.rss"})
    void test_when_items_from_valid_rss_url_extracted(final String url) {
        when(rssSourceConfig.getUrl()).thenReturn(url);
        doCallRealMethod().when(rssSource).extractItemsFromRssFeed(any(RSSSourceConfig.class));
        rssSource.extractItemsFromRssFeed(rssSourceConfig);
        verify(rssSource, times(1)).extractItemsFromRssFeed(rssSourceConfig);

    }

   @Test
    void test_when_empty_rss_url_provided() {
        when(rssSourceConfig.getUrl()).thenReturn("");
       rssSource.extractItemsFromRssFeed(rssSourceConfig);
       doThrow(IllegalArgumentException.class).when(rssSource).extractItemsFromRssFeed(any(RSSSourceConfig.class));
       assertThrows(IllegalArgumentException.class, () -> rssSource.extractItemsFromRssFeed(rssSourceConfig));
    }

    @Test
    void test_when_invalid_rss_url_provided() {
        when(rssSourceConfig.getUrl()).thenReturn(INVALID_RSS_URL);
        rssSource.extractItemsFromRssFeed(rssSourceConfig);
        doThrow(RuntimeException.class).when(rssSource).extractItemsFromRssFeed(isA(RSSSourceConfig.class));
        assertThrows(RuntimeException.class, () -> rssSource.extractItemsFromRssFeed(rssSourceConfig));
    }
}
