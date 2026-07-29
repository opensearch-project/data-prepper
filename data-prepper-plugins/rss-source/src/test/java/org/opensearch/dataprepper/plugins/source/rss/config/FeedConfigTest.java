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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class FeedConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new SimpleModule().addDeserializer(Duration.class, new DataPrepperDurationDeserializer()));

    @Test
    void deserializes_url_only_with_null_optionals() throws Exception {
        final FeedConfig config = objectMapper.readValue(
                "{\"url\":\"https://example.com/feed.xml\"}", FeedConfig.class);
        assertThat(config.getUrl(), equalTo("https://example.com/feed.xml"));
        assertThat(config.getPollingFrequency(), nullValue());
        assertThat(config.getAuthentication(), nullValue());
    }

    @Test
    void deserializes_per_feed_polling_and_auth() throws Exception {
        final String json = "{\"url\":\"https://x/f\",\"polling_frequency\":\"PT1M\","
                + "\"authentication\":{\"basic\":{\"username\":\"u\",\"password\":\"p\"}}}";
        final FeedConfig config = objectMapper.readValue(json, FeedConfig.class);
        assertThat(config.getPollingFrequency(), equalTo(Duration.ofMinutes(1)));
        assertThat(config.getAuthentication(), notNullValue());
        assertThat(config.getAuthentication().getBasic().getUsername(), equalTo("u"));
    }

    @Test
    void deserializes_simple_duration_notation() throws Exception {
        final FeedConfig config = objectMapper.readValue(
                "{\"url\":\"https://x/f\",\"polling_frequency\":\"60s\"}", FeedConfig.class);
        assertThat(config.getPollingFrequency(), equalTo(Duration.ofSeconds(60)));
    }
}
