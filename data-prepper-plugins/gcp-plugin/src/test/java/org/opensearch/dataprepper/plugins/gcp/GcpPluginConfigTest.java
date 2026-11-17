/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.gcp.api.GcpAuthMode;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GcpPluginConfigTest {
    private static final Validator VALIDATOR = Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory().getValidator();

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    @Test
    void deserialize_default_block() throws IOException {
        final GcpPluginConfig objectUnderTest =
                objectMapper.readValue("default:\n  project_id: p\n  auth_mode: adc\n", GcpPluginConfig.class);

        assertThat(objectUnderTest.getDefaultConfig().getProjectId(), equalTo("p"));
        assertThat(objectUnderTest.getDefaultConfig().getAuthMode(), equalTo(GcpAuthMode.ADC));
        assertThat(VALIDATOR.validate(objectUnderTest).isEmpty(), is(true));
    }

    @Test
    void deserialize_empty_config_has_no_default() throws IOException {
        assertThat(objectMapper.readValue("{}", GcpPluginConfig.class).getDefaultConfig(), nullValue());
    }

    @Test
    void validate_cascades_into_default_block() throws IOException {
        final GcpPluginConfig objectUnderTest = objectMapper.readValue(
                "default:\n  auth_mode: adc\n  client_email: sa@p.iam.gserviceaccount.com\n", GcpPluginConfig.class);

        final Set<ConstraintViolation<GcpPluginConfig>> violations = VALIDATOR.validate(objectUnderTest);

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(),
                equalTo("auth_mode adc cannot be combined with private_key, client_email or token_uri"));
    }

    @Test
    void deserialize_named_configuration_is_rejected() {
        assertThrows(UnrecognizedPropertyException.class,
                () -> objectMapper.readValue("other:\n  project_id: p\n", GcpPluginConfig.class));
    }
}
