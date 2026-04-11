/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class ShuffleConfigTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ShuffleConfig deserialize(final Map<String, Object> map) throws Exception {
        final String json = MAPPER.writeValueAsString(map);
        return MAPPER.readValue(json, ShuffleConfig.class);
    }

    @Test
    void defaults() throws Exception {
        final ShuffleConfig config = deserialize(Map.of());
        assertThat(config.getPartitions(), is(ShuffleConfig.DEFAULT_PARTITIONS));
        assertThat(config.getServerPort(), is(ShuffleConfig.DEFAULT_SERVER_PORT));
        assertThat(config.isSsl(), is(true));
        assertThat(config.isSslInsecureDisableVerification(), is(false));
    }

    @Test
    void ssl_disabled_does_not_require_cert_files() throws Exception {
        final ShuffleConfig config = deserialize(Map.of("ssl", false));
        assertThat(config.isSslCertificateFileValid(), is(true));
        assertThat(config.isSslKeyFileValid(), is(true));
    }

    @Test
    void ssl_enabled_without_cert_files_fails_validation() throws Exception {
        final ShuffleConfig config = deserialize(Map.of("ssl", true));
        assertThat(config.isSslCertificateFileValid(), is(false));
        assertThat(config.isSslKeyFileValid(), is(false));
    }

    @Test
    void ssl_enabled_with_cert_files_passes_validation() throws Exception {
        final ShuffleConfig config = deserialize(Map.of(
                "ssl", true,
                "ssl_certificate_file", "/path/to/cert.pem",
                "ssl_key_file", "/path/to/key.pem"
        ));
        assertThat(config.isSslCertificateFileValid(), is(true));
        assertThat(config.isSslKeyFileValid(), is(true));
    }

    @ParameterizedTest
    @CsvSource({"true", "false"})
    void ssl_insecure_disable_verification_deserialized(final boolean value) throws Exception {
        final ShuffleConfig config = deserialize(Map.of(
                "ssl", false,
                "ssl_insecure_disable_verification", value
        ));
        assertThat(config.isSslInsecureDisableVerification(), is(value));
    }

    @Test
    void authentication_is_deserialized() throws Exception {
        final ShuffleConfig config = deserialize(Map.of(
                "ssl", false,
                "authentication", Map.of("http_basic", Map.of("username", "admin", "password", "secret"))
        ));
        assertThat(config.getAuthentication().getPluginName(), equalTo("http_basic"));
    }

    @Test
    void authentication_defaults_to_null() throws Exception {
        final ShuffleConfig config = deserialize(Map.of("ssl", false));
        assertThat(config.getAuthentication(), is(nullValue()));
    }

    @Test
    void storage_path_is_deserialized() throws Exception {
        final ShuffleConfig config = deserialize(Map.of("ssl", false, "storage_path", "/custom/shuffle"));
        assertThat(config.getStoragePath(), equalTo("/custom/shuffle"));
    }

    @Test
    void storage_path_defaults_to_null() throws Exception {
        final ShuffleConfig config = deserialize(Map.of("ssl", false));
        assertThat(config.getStoragePath(), is(nullValue()));
    }
}
