/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsSecretsPluginConfigVariableTest {
    private final String secretId = "valid@secret-manager_name";
    private final String secretKey = UUID.randomUUID().toString();
    private final Object secretValue = UUID.randomUUID().toString();
    @Mock
    private SecretsSupplier secretsSupplier;
    private AwsPluginConfigVariable objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new AwsPluginConfigVariable(
                secretsSupplier,
                secretId, secretKey,
                secretValue
        );
    }

    @Test
    void testGetPrefix() {
        assertThat(objectUnderTest.getValue(), equalTo(secretValue));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "new-secret-to-set"
    })
    void testSetValueFailure(final String input) {
        when(secretsSupplier.updateValue(secretId, secretKey, input)).thenThrow(RuntimeException.class);
        assertThrows(RuntimeException.class, () -> objectUnderTest.setValue(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "new-secret-to-set"
    })
    void testSetValueSuccess(final String input) {
        when(secretsSupplier.updateValue(secretId, secretKey, input)).thenReturn("new-version-id");
        objectUnderTest.setValue(input);
        assertThat(objectUnderTest.getValue(), equalTo(input));
    }

}