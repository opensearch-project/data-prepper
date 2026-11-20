/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class GcpConfigTest {
    private static final Validator VALIDATOR = Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory().getValidator();
    private static final String KEY_BLOCK = "auth_mode: service_account_key\nclient_email: sa@p.iam.gserviceaccount.com\n"
            + "private_key: \"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"\n";

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    private GcpConfig read(final String yaml) throws IOException {
        return objectMapper.readValue(yaml, GcpConfig.class);
    }

    @Test
    void deserialize_empty_block_has_no_mode_and_default_options() throws IOException {
        final GcpConfig objectUnderTest = read("{}");

        assertThat(objectUnderTest.getAuthMode(), nullValue());
        assertThat(objectUnderTest.getProjectId(), nullValue());
        assertThat(objectUnderTest.getScopes(), nullValue());
        assertThat(objectUnderTest.getTokenUri(), nullValue());
        assertThat(objectUnderTest.getClientEmail(), nullValue());
        assertThat(VALIDATOR.validate(objectUnderTest).isEmpty(), is(true));
        assertThat(objectUnderTest.toCredentialsOptions(), equalTo(GcpCredentialsOptions.builder().build()));
    }

    @Test
    void deserialize_adc_block_maps_to_options() throws IOException {
        final GcpConfig objectUnderTest = read("project_id: my-project\nauth_mode: adc\nscopes: scope-a\n");

        assertThat(VALIDATOR.validate(objectUnderTest).isEmpty(), is(true));
        assertThat(objectUnderTest.getScopes(), equalTo(List.of("scope-a")));
        assertThat(objectUnderTest.toCredentialsOptions(), equalTo(GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.ADC).withProjectId("my-project").withScopes(List.of("scope-a")).build()));
    }

    @Test
    void deserialize_service_account_key_block_maps_to_options() throws IOException {
        final GcpConfig objectUnderTest = read(KEY_BLOCK + "token_uri: https://oauth2.googleapis.com/custom\nscopes:\n  - b\n  - a\n");

        assertThat(VALIDATOR.validate(objectUnderTest).isEmpty(), is(true));
        final GcpCredentialsOptions options = objectUnderTest.toCredentialsOptions();
        assertThat(options.getAuthMode(), equalTo(GcpAuthMode.SERVICE_ACCOUNT_KEY));
        assertThat(options.getClientEmail(), equalTo("sa@p.iam.gserviceaccount.com"));
        assertThat(options.getPrivateKey().startsWith("-----BEGIN PRIVATE KEY-----"), is(true));
        assertThat(options.getScopes(), equalTo(List.of("a", "b")));
        assertThat(options.getTokenUri(), equalTo("https://oauth2.googleapis.com/custom"));
    }

    @Test
    void validate_adc_with_key_fields_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations =
                VALIDATOR.validate(read("auth_mode: adc\nclient_email: sa@p.iam.gserviceaccount.com\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(),
                equalTo(GcpCredentialsRules.ADC_WITH_KEY_FIELDS));
    }

    @Test
    void validate_key_fields_without_mode_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations = VALIDATOR.validate(read("client_email: sa@p.iam.gserviceaccount.com\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(),
                equalTo(GcpCredentialsRules.KEY_FIELDS_WITHOUT_MODE));
    }

    @Test
    void validate_service_account_key_missing_private_key_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations =
                VALIDATOR.validate(read("auth_mode: service_account_key\nclient_email: sa@p.iam.gserviceaccount.com\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(),
                equalTo(GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY));
    }

    @Test
    void validate_service_account_key_missing_client_email_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations =
                VALIDATOR.validate(read("auth_mode: service_account_key\nprivate_key: abc\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(), equalTo(GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY));
    }

    @Test
    void validate_empty_project_id_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations = VALIDATOR.validate(read("project_id: \"\"\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(), equalTo("project_id cannot be empty"));
    }

    @Test
    void validate_private_key_without_mode_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations = VALIDATOR.validate(read("private_key: abc\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(),
                equalTo(GcpCredentialsRules.KEY_FIELDS_WITHOUT_MODE));
    }

    @Test
    void validate_adc_with_token_uri_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations =
                VALIDATOR.validate(read("auth_mode: adc\ntoken_uri: https://oauth2.googleapis.com/token\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(), equalTo(GcpCredentialsRules.ADC_WITH_KEY_FIELDS));
    }

    @Test
    void validate_service_account_key_with_blank_private_key_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations = VALIDATOR.validate(read(
                "auth_mode: service_account_key\nclient_email: sa@p.iam.gserviceaccount.com\nprivate_key: \" \"\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(), equalTo(GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY));
    }

    @Test
    void validate_token_uri_without_mode_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations =
                VALIDATOR.validate(read("token_uri: https://oauth2.googleapis.com/token\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(), equalTo(GcpCredentialsRules.KEY_FIELDS_WITHOUT_MODE));
    }

    @Test
    void validate_service_account_key_with_blank_client_email_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> violations = VALIDATOR.validate(read(
                "auth_mode: service_account_key\nclient_email: \" \"\nprivate_key: abc\n"));

        assertThat(violations.size(), equalTo(1));
        assertThat(violations.iterator().next().getMessage(), equalTo(GcpCredentialsRules.INCOMPLETE_SERVICE_ACCOUNT_KEY));
    }

    @Test
    void validate_null_or_blank_scope_has_violation() throws IOException {
        final Set<ConstraintViolation<GcpConfig>> nullScope = VALIDATOR.validate(read("scopes: [ ~ ]\n"));
        final Set<ConstraintViolation<GcpConfig>> blankScope = VALIDATOR.validate(read("scopes: [ \" \" ]\n"));

        assertThat(nullScope.size(), equalTo(1));
        assertThat(nullScope.iterator().next().getMessage(), equalTo(GcpCredentialsRules.INVALID_SCOPES));
        assertThat(blankScope.size(), equalTo(1));
        assertThat(blankScope.iterator().next().getMessage(), equalTo(GcpCredentialsRules.INVALID_SCOPES));
    }
}
