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
    void testDefaultSkipValidationOnStart() {
        final AwsSecretManagerConfiguration config = new AwsSecretManagerConfiguration();
        
        // Default should be false (validate by default)
        assertThat(config.isSkipValidationOnStart(), equalTo(false));
    }

    @Test
    void testSkipValidationOnStartFromYaml_Enabled() throws Exception {
        final String yaml = 
            "secret_id: my-secret\n" +
            "region: us-east-1\n" +
            "refresh_interval: PT1H\n" +
            "skip_validation_on_start: true\n";
        
        final AwsSecretManagerConfiguration config = 
            objectMapper.readValue(yaml, AwsSecretManagerConfiguration.class);
        
        assertThat(config.isSkipValidationOnStart(), equalTo(true));
    }

    @Test
    void testSkipValidationOnStartFromYaml_Disabled() throws Exception {
        final String yaml = 
            "secret_id: my-secret\n" +
            "region: us-east-1\n" +
            "refresh_interval: PT1H\n" +
            "skip_validation_on_start: false\n";
        
        final AwsSecretManagerConfiguration config = 
            objectMapper.readValue(yaml, AwsSecretManagerConfiguration.class);
        
        assertThat(config.isSkipValidationOnStart(), equalTo(false));
    }
}
