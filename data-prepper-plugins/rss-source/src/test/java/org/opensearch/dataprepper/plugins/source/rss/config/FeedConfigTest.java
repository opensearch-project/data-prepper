/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class FeedConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

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
}
