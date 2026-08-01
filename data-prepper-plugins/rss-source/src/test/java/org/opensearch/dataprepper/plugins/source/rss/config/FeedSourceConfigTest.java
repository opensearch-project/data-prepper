/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.source.rss.config.FeedSourceConfig.DEFAULT_POLLING_FREQUENCY;

class FeedSourceConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new SimpleModule().addDeserializer(Duration.class, new DataPrepperDurationDeserializer()));

    @Test
    void defaults_are_applied() {
        final FeedSourceConfig config = new FeedSourceConfig();
        assertThat(config.getPollingFrequency(), equalTo(DEFAULT_POLLING_FREQUENCY));
        assertThat(config.getWorkers(), equalTo(1));
    }

    @Test
    void deserializes_feeds_map_and_globals() throws Exception {
        final String json = "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"},\"b\":{\"url\":\"https://b/f\"}},"
                + "\"polling_frequency\":\"PT10M\",\"workers\":3}";
        final FeedSourceConfig config = objectMapper.readValue(json, FeedSourceConfig.class);
        assertThat(config.getFeeds().size(), equalTo(2));
        assertThat(config.getFeeds().get("a").getUrl(), equalTo("https://a/f"));
        assertThat(config.getFeeds().get("b").getUrl(), equalTo("https://b/f"));
        assertThat(config.getPollingFrequency(), equalTo(Duration.ofMinutes(10)));
        assertThat(config.getWorkers(), equalTo(3));
    }

    @Test
    void resolvePollingFrequency_uses_feed_override_then_falls_back_to_global() {
        final FeedSourceConfig config = new FeedSourceConfig();
        final FeedConfig withOverride = new FeedConfig() {
            @Override
            public Duration getPollingFrequency() {
                return Duration.ofMinutes(1);
            }
        };
        final FeedConfig noOverride = new FeedConfig();
        assertThat(config.resolvePollingFrequency(withOverride), equalTo(Duration.ofMinutes(1)));
        assertThat(config.resolvePollingFrequency(noOverride), equalTo(DEFAULT_POLLING_FREQUENCY));
    }

    @Test
    void polling_frequency_at_or_above_one_second_is_valid() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"polling_frequency\":\"1s\"}",
                FeedSourceConfig.class);
        assertThat(config.isPollingFrequencyAboveMinimum(), equalTo(true));
    }

    @Test
    void global_polling_frequency_below_one_second_is_invalid() throws Exception {
        final FeedSourceConfig config = objectMapper.readValue(
                "{\"feeds\":{\"a\":{\"url\":\"https://a/f\"}},\"polling_frequency\":\"500ms\"}",
                FeedSourceConfig.class);
        assertThat(config.isPollingFrequencyAboveMinimum(), equalTo(false));
    }

    @Test
    void per_feed_polling_frequency_below_one_second_is_invalid() throws Exception {
        final String json = "{\"feeds\":{\"a\":{\"url\":\"https://a/f\",\"polling_frequency\":\"0s\"}},"
                + "\"polling_frequency\":\"PT5M\"}";
        final FeedSourceConfig config = objectMapper.readValue(json, FeedSourceConfig.class);
        assertThat(config.isPollingFrequencyAboveMinimum(), equalTo(false));
    }
}
