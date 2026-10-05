/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

class AzureFederatedAuthConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String CLIENT_ID = "11111111-1111-1111-1111-111111111111";
    private static final String TOKEN_ENDPOINT =
            "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/v2.0/token";
    private static final String SCOPE = "https://my-namespace.servicebus.windows.net/.default";

    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();
    }

    @Test
    void allFieldsDeserializeAndGettersReturnThemVerbatim() {
        final AzureFederatedAuthConfig config = OBJECT_MAPPER.convertValue(fullConfig(), AzureFederatedAuthConfig.class);

        assertThat(config, notNullValue());
        assertThat(config.getClientId(), equalTo(CLIENT_ID));
        assertThat(config.getTokenEndpoint(), equalTo(TOKEN_ENDPOINT));
        assertThat(config.getScope(), equalTo(SCOPE));
    }

    @Test
    void clientIdIsRequired() {
        assertMissingFieldIsRejected("client_id", "clientId",
                "client_id is required for azure_federated authentication");
    }

    @Test
    void tokenEndpointIsRequired() {
        assertMissingFieldIsRejected("token_endpoint", "tokenEndpoint",
                "token_endpoint is required for azure_federated authentication");
    }

    @Test
    void scopeIsRequired() {
        assertMissingFieldIsRejected("scope", "scope",
                "scope is required for azure_federated authentication");
    }

    @Test
    void allFieldsPresent_isValid() {
        final AzureFederatedAuthConfig config = OBJECT_MAPPER.convertValue(fullConfig(), AzureFederatedAuthConfig.class);

        final Set<ConstraintViolation<AzureFederatedAuthConfig>> violations = validator.validate(config);

        assertThat(violations, empty());
    }

    private void assertMissingFieldIsRejected(final String jsonProperty, final String propertyPath,
                                              final String expectedMessage) {
        final Map<String, String> yaml = fullConfig();
        yaml.remove(jsonProperty);
        final AzureFederatedAuthConfig config = OBJECT_MAPPER.convertValue(yaml, AzureFederatedAuthConfig.class);

        final Set<ConstraintViolation<AzureFederatedAuthConfig>> violations = validator.validate(config);

        assertThat(violations, hasSize(1));
        final ConstraintViolation<AzureFederatedAuthConfig> violation = violations.iterator().next();
        assertThat(violation.getPropertyPath().toString(), equalTo(propertyPath));
        assertThat(violation.getMessage(), equalTo(expectedMessage));
    }

    private Map<String, String> fullConfig() {
        final Map<String, String> yaml = new HashMap<>();
        yaml.put("client_id", CLIENT_ID);
        yaml.put("token_endpoint", TOKEN_ENDPOINT);
        yaml.put("scope", SCOPE);
        return yaml;
    }
}
