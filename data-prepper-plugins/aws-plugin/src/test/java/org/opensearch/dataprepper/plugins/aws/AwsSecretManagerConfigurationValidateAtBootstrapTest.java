/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class AwsSecretManagerConfigurationValidateAtBootstrapTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory())
            .registerModule(new JavaTimeModule());

    @Test
    void testDefaultValidateAtBootstrap() {
        final AwsSecretManagerConfiguration config = new AwsSecretManagerConfiguration();
        
        // Default should be true (safe-by-default)
        assertThat(config.isValidateAtBootstrap(), equalTo(true));
    }

    @Test
    void testValidateAtBootstrapFromYaml_Enabled() throws Exception {
        final String yaml = 
            "secret_id: my-secret\n" +
            "region: us-east-1\n" +
            "refresh_interval: PT1H\n" +
            "validate_at_bootstrap: true\n";
        
        final AwsSecretManagerConfiguration config = 
            objectMapper.readValue(yaml, AwsSecretManagerConfiguration.class);
        
        assertThat(config.isValidateAtBootstrap(), equalTo(true));
    }

    @Test
    void testValidateAtBootstrapFromYaml_Disabled() throws Exception {
        final String yaml = 
            "secret_id: my-secret\n" +
            "region: us-east-1\n" +
            "refresh_interval: PT1H\n" +
            "validate_at_bootstrap: false\n";
        
        final AwsSecretManagerConfiguration config = 
            objectMapper.readValue(yaml, AwsSecretManagerConfiguration.class);
        
        assertThat(config.isValidateAtBootstrap(), equalTo(false));
    }

    @Test
    void testValidateAtBootstrapFromYaml_NotSpecified_UsesDefault() throws Exception {
        final String yaml = 
            "secret_id: my-secret\n" +
            "region: us-east-1\n" +
            "refresh_interval: PT1H\n";
        
        final AwsSecretManagerConfiguration config = 
            objectMapper.readValue(yaml, AwsSecretManagerConfiguration.class);
        
        // Should default to true when not specified
        assertThat(config.isValidateAtBootstrap(), equalTo(true));
    }
}
