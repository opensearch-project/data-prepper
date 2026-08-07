/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.http.client.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class HttpClientAuthenticationConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDefaultHttpBasicIsNull() {
        final HttpClientAuthenticationConfig config = new HttpClientAuthenticationConfig();
        assertThat(config.getHttpBasic(), nullValue());
    }

    @Test
    void testDefaultBearerTokenIsNull() {
        final HttpClientAuthenticationConfig config = new HttpClientAuthenticationConfig();
        assertThat(config.getBearerToken(), nullValue());
    }

    @Test
    void testDeserializationWithBearerToken() throws Exception {
        final String json = "{\"bearer_token\": \"my-secret-token\"}";
        final HttpClientAuthenticationConfig config = OBJECT_MAPPER.readValue(json, HttpClientAuthenticationConfig.class);

        assertThat(config.getBearerToken(), equalTo("my-secret-token"));
        assertThat(config.getHttpBasic(), nullValue());
    }

    @Test
    void testDeserializationWithHttpBasic() throws Exception {
        final String json = "{\"http_basic\": {\"username\": \"admin\", \"password\": \"secret123\"}}";
        final HttpClientAuthenticationConfig config = OBJECT_MAPPER.readValue(json, HttpClientAuthenticationConfig.class);

        assertThat(config.getHttpBasic(), notNullValue());
        assertThat(config.getHttpBasic().getUsername(), equalTo("admin"));
        assertThat(config.getHttpBasic().getPassword(), equalTo("secret123"));
        assertThat(config.getBearerToken(), nullValue());
    }

    @Test
    void testDeserializationWithEmptyJson() throws Exception {
        final String json = "{}";
        final HttpClientAuthenticationConfig config = OBJECT_MAPPER.readValue(json, HttpClientAuthenticationConfig.class);

        assertThat(config.getHttpBasic(), nullValue());
        assertThat(config.getBearerToken(), nullValue());
    }

    @Test
    void testHttpBasicConfigDefaultUsernameIsNull() {
        final HttpClientAuthenticationConfig.HttpBasicConfig httpBasicConfig =
                new HttpClientAuthenticationConfig.HttpBasicConfig();
        assertThat(httpBasicConfig.getUsername(), nullValue());
    }

    @Test
    void testHttpBasicConfigDefaultPasswordIsNull() {
        final HttpClientAuthenticationConfig.HttpBasicConfig httpBasicConfig =
                new HttpClientAuthenticationConfig.HttpBasicConfig();
        assertThat(httpBasicConfig.getPassword(), nullValue());
    }
}
