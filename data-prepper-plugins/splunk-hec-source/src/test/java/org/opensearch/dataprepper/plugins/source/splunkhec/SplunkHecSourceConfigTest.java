/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SplunkHecSourceConfigTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new JavaTimeModule());

    @Test
    void default_port_is_8088() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.getDefaultPort(), equalTo(8088));
    }

    @Test
    void default_path_is_services_collector() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.getDefaultPath(), equalTo("/services/collector"));
    }

    @Test
    void hasHealthCheckService_returns_false() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.hasHealthCheckService(), is(false));
    }

    @Test
    void default_flatten_event_is_true() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.isFlattenEvent(), is(true));
    }

    @Test
    void default_raw_line_breaker_is_newline() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.getRawLineBreaker(), equalTo("\n"));
    }

    @Test
    void default_sourcetype_is_httpevent() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.getDefaultSourcetype(), equalTo("httpevent"));
    }

    @Test
    void default_warn_future_timestamps_is_false() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.isWarnFutureTimestamps(), is(false));
    }

    @Test
    void default_acknowledgements_is_false() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.isAcknowledgements(), is(false));
    }

    @Test
    void default_ack_expiry_is_300_seconds() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.getAckExpiry(), equalTo(Duration.ofSeconds(300)));
    }

    @Test
    void default_use_forwarded_headers_is_false() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.isUseForwardedHeaders(), is(false));
    }

    @Test
    void deserialization_with_tokens() throws IOException {
        final String yaml = "tokens:\n"
                + "  - token: \"test-token-123\"\n"
                + "    defaults:\n"
                + "      index: \"main\"\n"
                + "      sourcetype: \"syslog\"\n"
                + "  - token: \"test-token-456\"\n";

        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);

        assertThat(config.getTokens(), hasSize(2));
        assertThat(config.getTokens().get(0).getToken(), equalTo("test-token-123"));
        assertThat(config.getTokens().get(0).getDefaults(), notNullValue());
        assertThat(config.getTokens().get(0).getDefaults().getIndex(), equalTo("main"));
        assertThat(config.getTokens().get(0).getDefaults().getSourcetype(), equalTo("syslog"));
        assertThat(config.getTokens().get(1).getToken(), equalTo("test-token-456"));
    }

    @Test
    void deserialization_with_all_fields() throws IOException {
        final String yaml = "tokens:\n"
                + "  - token: \"my-token\"\n"
                + "flatten_event: false\n"
                + "raw_line_breaker: \"\\r\\n\"\n"
                + "default_sourcetype: \"custom\"\n"
                + "warn_future_timestamps: true\n"
                + "acknowledgements: true\n"
                + "ack_expiry: \"PT600S\"\n"
                + "use_forwarded_headers: true\n";

        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);

        assertThat(config.isFlattenEvent(), is(false));
        assertThat(config.getDefaultSourcetype(), equalTo("custom"));
        assertThat(config.isWarnFutureTimestamps(), is(true));
        assertThat(config.isAcknowledgements(), is(true));
        assertThat(config.getAckExpiry(), equalTo(Duration.ofSeconds(600)));
        assertThat(config.isUseForwardedHeaders(), is(true));
    }

    @Test
    void isTokensValid_returns_true_for_valid_tokens() throws IOException {
        final String yaml = "tokens:\n"
                + "  - token: \"valid-token-1\"\n"
                + "  - token: \"valid-token-2\"\n";
        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);
        assertThat(config.isTokensValid(), is(true));
    }

    @Test
    void isTokensValid_returns_false_for_blank_token() throws IOException {
        final String yaml = "tokens:\n"
                + "  - token: \"  \"\n";
        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);
        assertThat(config.isTokensValid(), is(false));
    }

    @Test
    void isTokensValid_returns_false_for_null_list_element() throws IOException {
        final String yaml = "tokens:\n"
                + "  - token: \"valid-token\"\n"
                + "  - ~\n";
        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);
        assertThat(config.isTokensValid(), is(false));
    }

    @Test
    void getTokens_returns_unmodifiable_list() throws IOException {
        final String yaml = "tokens:\n"
                + "  - token: \"test-token\"\n";
        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);
        assertThat(config.getTokens(), hasSize(1));
        assertThrows(UnsupportedOperationException.class, () -> config.getTokens().clear());
    }

    @Test
    void isTokensValid_returns_false_when_tokens_null() throws IOException {
        final String yaml = "tokens: null\n";
        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);
        assertThat(config.isTokensValid(), is(false));
    }

    @Test
    void getTokens_returns_empty_list_when_tokens_null() throws IOException {
        final String yaml = "tokens: null\n";
        final SplunkHecSourceConfig config = YAML_MAPPER.readValue(yaml, SplunkHecSourceConfig.class);
        assertThat(config.getTokens(), hasSize(0));
    }

    @Test
    void isAckExpiryPositive_returns_true_for_positive() throws IOException {
        final SplunkHecSourceConfig config = YAML_MAPPER.readValue("tokens:\n  - token: \"t\"\n", SplunkHecSourceConfig.class);
        assertThat(config.isAckExpiryPositive(), is(true));
    }

    @Test
    void isAckExpiryPositive_returns_false_for_negative() throws IOException {
        final SplunkHecSourceConfig config =
                YAML_MAPPER.readValue("tokens:\n  - token: \"t\"\nack_expiry: \"-PT10S\"\n", SplunkHecSourceConfig.class);
        assertThat(config.isAckExpiryPositive(), is(false));
    }

    @Test
    void isAckExpiryPositive_returns_false_for_zero() throws IOException {
        final SplunkHecSourceConfig config =
                YAML_MAPPER.readValue("tokens:\n  - token: \"t\"\nack_expiry: \"PT0S\"\n", SplunkHecSourceConfig.class);
        assertThat(config.isAckExpiryPositive(), is(false));
    }

    @Test
    void isAckExpiryPositive_returns_true_when_null() throws IOException {
        final SplunkHecSourceConfig config =
                YAML_MAPPER.readValue("tokens:\n  - token: \"t\"\nack_expiry: null\n", SplunkHecSourceConfig.class);
        assertThat(config.isAckExpiryPositive(), is(true));
    }

    @Test
    void getPort_returns_default_port() {
        final SplunkHecSourceConfig config = new SplunkHecSourceConfig();
        assertThat(config.getPort(), equalTo(8088));
    }
}
