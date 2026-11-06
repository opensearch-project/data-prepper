/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

class HecTokenConfigTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    @Test
    void deserialization_with_token_and_defaults() throws IOException {
        final String yaml = "token: \"my-token-value\"\n"
                + "defaults:\n"
                + "  index: \"main\"\n"
                + "  sourcetype: \"syslog\"\n"
                + "  source: \"forwarder\"\n"
                + "  host: \"host1\"\n"
                + "  fields:\n"
                + "    env: \"prod\"\n"
                + "    region: \"us-east\"\n";

        final HecTokenConfig config = YAML_MAPPER.readValue(yaml, HecTokenConfig.class);

        assertThat(config.getToken(), equalTo("my-token-value"));
        assertThat(config.getDefaults(), notNullValue());
        assertThat(config.getDefaults().getIndex(), equalTo("main"));
        assertThat(config.getDefaults().getSourcetype(), equalTo("syslog"));
        assertThat(config.getDefaults().getSource(), equalTo("forwarder"));
        assertThat(config.getDefaults().getHost(), equalTo("host1"));
        assertThat(config.getDefaults().getFields(), hasEntry("env", "prod"));
        assertThat(config.getDefaults().getFields(), hasEntry("region", "us-east"));
    }

    @Test
    void deserialization_with_token_only() throws IOException {
        final String yaml = "token: \"simple-token\"\n";

        final HecTokenConfig config = YAML_MAPPER.readValue(yaml, HecTokenConfig.class);

        assertThat(config.getToken(), equalTo("simple-token"));
        assertThat(config.getDefaults(), nullValue());
    }

    @Test
    void enabled_defaults_to_true_and_can_be_disabled() throws IOException {
        final HecTokenConfig enabledByDefault = YAML_MAPPER.readValue("token: \"t\"\n", HecTokenConfig.class);
        assertThat(enabledByDefault.isEnabled(), equalTo(true));

        final HecTokenConfig disabled = YAML_MAPPER.readValue("token: \"t\"\nenabled: false\n", HecTokenConfig.class);
        assertThat(disabled.isEnabled(), equalTo(false));
    }

    @Test
    void getFields_returns_empty_map_when_null() throws IOException {
        final String yaml = "token: \"t\"\ndefaults:\n  index: \"main\"\n";
        final HecTokenConfig config = YAML_MAPPER.readValue(yaml, HecTokenConfig.class);

        assertThat(config.getDefaults().getFields(), notNullValue());
        assertThat(config.getDefaults().getFields().entrySet(), hasSize(0));
    }

    @Test
    void equals_returns_true_for_same_token() throws IOException {
        final HecTokenConfig config1 = YAML_MAPPER.readValue("token: \"abc\"", HecTokenConfig.class);
        final HecTokenConfig config2 = YAML_MAPPER.readValue("token: \"abc\"", HecTokenConfig.class);

        assertThat(config1, equalTo(config2));
        assertThat(config1.hashCode(), equalTo(config2.hashCode()));
    }

    @Test
    void equals_returns_false_for_different_token() throws IOException {
        final HecTokenConfig config1 = YAML_MAPPER.readValue("token: \"abc\"", HecTokenConfig.class);
        final HecTokenConfig config2 = YAML_MAPPER.readValue("token: \"xyz\"", HecTokenConfig.class);

        assertThat(config1, not(equalTo(config2)));
    }

    @Test
    void equals_returns_false_for_null() throws IOException {
        final HecTokenConfig config = YAML_MAPPER.readValue("token: \"abc\"", HecTokenConfig.class);
        assertThat(config, not(equalTo(null)));
    }

    @Test
    void equals_returns_true_for_same_instance() throws IOException {
        final HecTokenConfig config = YAML_MAPPER.readValue("token: \"abc\"", HecTokenConfig.class);
        assertThat(config, equalTo(config));
    }
}
