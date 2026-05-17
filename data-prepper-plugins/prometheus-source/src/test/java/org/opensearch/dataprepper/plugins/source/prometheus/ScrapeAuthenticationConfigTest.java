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
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class ScrapeAuthenticationConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDefaultHttpBasicIsNull() {
        final ScrapeAuthenticationConfig config = new ScrapeAuthenticationConfig();
        assertThat(config.getHttpBasic(), nullValue());
    }

    @Test
    void testDefaultBearerTokenIsNull() {
        final ScrapeAuthenticationConfig config = new ScrapeAuthenticationConfig();
        assertThat(config.getBearerToken(), nullValue());
    }

    @Test
    void testDeserializationWithBearerToken() throws Exception {
        final String json = "{\"bearer_token\": \"my-secret-token\"}";

        final ScrapeAuthenticationConfig config = OBJECT_MAPPER.readValue(json, ScrapeAuthenticationConfig.class);

        assertThat(config.getBearerToken(), equalTo("my-secret-token"));
        assertThat(config.getHttpBasic(), nullValue());
    }

    @Test
    void testDeserializationWithHttpBasic() throws Exception {
        final String json = "{\"http_basic\": {\"username\": \"admin\", \"password\": \"secret123\"}}";

        final ScrapeAuthenticationConfig config = OBJECT_MAPPER.readValue(json, ScrapeAuthenticationConfig.class);

        assertThat(config.getHttpBasic(), notNullValue());
        assertThat(config.getHttpBasic().getUsername(), equalTo("admin"));
        assertThat(config.getHttpBasic().getPassword(), equalTo("secret123"));
        assertThat(config.getBearerToken(), nullValue());
    }

    @Test
    void testHttpBasicConfigDefaultUsernameIsNull() {
        final ScrapeAuthenticationConfig.HttpBasicConfig httpBasicConfig = new ScrapeAuthenticationConfig.HttpBasicConfig();
        assertThat(httpBasicConfig.getUsername(), nullValue());
    }

    @Test
    void testHttpBasicConfigDefaultPasswordIsNull() {
        final ScrapeAuthenticationConfig.HttpBasicConfig httpBasicConfig = new ScrapeAuthenticationConfig.HttpBasicConfig();
        assertThat(httpBasicConfig.getPassword(), nullValue());
    }

    @Test
    void testDeserializationWithHttpBasicOnly() throws Exception {
        final String json = "{\"http_basic\": {\"username\": \"user1\", \"password\": \"pass1\"}}";

        final ScrapeAuthenticationConfig config = OBJECT_MAPPER.readValue(json, ScrapeAuthenticationConfig.class);

        assertThat(config.getHttpBasic(), notNullValue());
        assertThat(config.getHttpBasic().getUsername(), equalTo("user1"));
        assertThat(config.getHttpBasic().getPassword(), equalTo("pass1"));
    }

    @Test
    void testDeserializationWithEmptyJson() throws Exception {
        final String json = "{}";

        final ScrapeAuthenticationConfig config = OBJECT_MAPPER.readValue(json, ScrapeAuthenticationConfig.class);

        assertThat(config.getHttpBasic(), nullValue());
        assertThat(config.getBearerToken(), nullValue());
    }
}