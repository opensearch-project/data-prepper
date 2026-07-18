/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class AuthenticationConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void basic_auth_deserializes_username_and_password() throws Exception {
        final String json = "{\"basic\":{\"username\":\"user\",\"password\":\"pass\"}}";
        final AuthenticationConfig config = objectMapper.readValue(json, AuthenticationConfig.class);
        assertThat(config.getBasic(), notNullValue());
        assertThat(config.getBasic().getUsername(), equalTo("user"));
        assertThat(config.getBasic().getPassword(), equalTo("pass"));
    }

    @Test
    void basic_is_null_when_absent() throws Exception {
        final AuthenticationConfig config = objectMapper.readValue("{}", AuthenticationConfig.class);
        assertThat(config.getBasic(), equalTo(null));
    }
}
