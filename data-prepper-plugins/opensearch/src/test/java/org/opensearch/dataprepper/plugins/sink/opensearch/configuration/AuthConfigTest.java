/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class AuthConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDefaultsAreNull() {
        final AuthConfig config = new AuthConfig();
        assertThat(config.getUsername(), nullValue());
        assertThat(config.getPassword(), nullValue());
        assertThat(config.getApitoken(), nullValue());
        assertThat(config.getClientCertificate(), nullValue());
        assertThat(config.getClientKey(), nullValue());
    }

    @Test
    void testDeserializationWithUsernamePassword() throws Exception {
        final String json = "{\"username\": \"admin\", \"password\": \"secret\"}";
        final AuthConfig config = OBJECT_MAPPER.readValue(json, AuthConfig.class);
        assertThat(config.getUsername(), equalTo("admin"));
        assertThat(config.getPassword(), equalTo("secret"));
        assertThat(config.getClientCertificate(), nullValue());
        assertThat(config.getClientKey(), nullValue());
    }

    @Test
    void testDeserializationWithClientCertificateAndKey() throws Exception {
        final String json = "{\"client_certificate\": \"-----BEGIN CERTIFICATE-----\\ntest\\n-----END CERTIFICATE-----\","
                + "\"client_key\": \"-----BEGIN PRIVATE KEY-----\\ntest\\n-----END PRIVATE KEY-----\"}";
        final AuthConfig config = OBJECT_MAPPER.readValue(json, AuthConfig.class);
        assertThat(config.getClientCertificate(), notNullValue());
        assertThat(config.getClientKey(), notNullValue());
        assertThat(config.getUsername(), nullValue());
    }

    @Test
    void testDeserializationWithAllFields() throws Exception {
        final String json = "{\"username\": \"admin\", \"password\": \"secret\","
                + "\"client_certificate\": \"/path/to/cert.pem\","
                + "\"client_key\": \"/path/to/key.pem\"}";
        final AuthConfig config = OBJECT_MAPPER.readValue(json, AuthConfig.class);
        assertThat(config.getUsername(), equalTo("admin"));
        assertThat(config.getPassword(), equalTo("secret"));
        assertThat(config.getClientCertificate(), equalTo("/path/to/cert.pem"));
        assertThat(config.getClientKey(), equalTo("/path/to/key.pem"));
    }

    @Test
    void testIsClientCertificateValidWhenBothSet() throws Exception {
        final String json = "{\"client_certificate\": \"/path/to/cert.pem\", \"client_key\": \"/path/to/key.pem\"}";
        final AuthConfig config = OBJECT_MAPPER.readValue(json, AuthConfig.class);
        assertThat(config.isClientCertificateValid(), is(true));
    }

    @Test
    void testIsClientCertificateValidWhenNeitherSet() {
        final AuthConfig config = new AuthConfig();
        assertThat(config.isClientCertificateValid(), is(true));
    }

    @Test
    void testIsClientCertificateValidWhenOnlyCertSet() throws Exception {
        final String json = "{\"client_certificate\": \"/path/to/cert.pem\"}";
        final AuthConfig config = OBJECT_MAPPER.readValue(json, AuthConfig.class);
        assertThat(config.isClientCertificateValid(), is(false));
    }

    @Test
    void testIsClientCertificateValidWhenOnlyKeySet() throws Exception {
        final String json = "{\"client_key\": \"/path/to/key.pem\"}";
        final AuthConfig config = OBJECT_MAPPER.readValue(json, AuthConfig.class);
        assertThat(config.isClientCertificateValid(), is(false));
    }

    @Test
    void testDeserializationWithApiToken() throws Exception {
        final String json = "{\"api_token\": \"abc123\"}";
        final AuthConfig config = OBJECT_MAPPER.readValue(json, AuthConfig.class);
        assertThat(config.getApitoken(), equalTo("abc123"));
    }
}
