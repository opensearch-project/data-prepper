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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class ScrapeTargetConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDefaultUrlIsNull() {
        final ScrapeTargetConfig config = new ScrapeTargetConfig();
        assertThat(config.getUrl(), nullValue());
    }

    @Test
    void testDeserializationFromJson() throws Exception {
        final String json = "{\"url\": \"http://localhost:9090/metrics\"}";

        final ScrapeTargetConfig config = OBJECT_MAPPER.readValue(json, ScrapeTargetConfig.class);

        assertThat(config.getUrl(), equalTo("http://localhost:9090/metrics"));
    }

    @Test
    void testDeserializationWithDifferentUrl() throws Exception {
        final String json = "{\"url\": \"https://prometheus.example.com:8080/api/v1/metrics\"}";

        final ScrapeTargetConfig config = OBJECT_MAPPER.readValue(json, ScrapeTargetConfig.class);

        assertThat(config.getUrl(), equalTo("https://prometheus.example.com:8080/api/v1/metrics"));
    }
}