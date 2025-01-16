/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.FailedToUpdateSecretException;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwsPluginConfigVariableTest {
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
                secretValue, true
        );
    }

    @Test
    void testGetPrefix() {
        assertThat(objectUnderTest.getValue(), equalTo(secretValue));
    }

    @Test
    void testSetValueFailure_when_secret_is_not_updatable() {
        objectUnderTest = new AwsPluginConfigVariable(
                secretsSupplier,
                secretId, secretKey,
                secretValue, false
        );
        assertThrows(FailedToUpdateSecretException.class, () -> objectUnderTest.setValue("new-secret-to-set", "randomSecretId"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "new-secret-to-set"
    })
    void testSetValueFailure(final String input) {
        when(secretsSupplier.updateValue(secretId, secretKey, input)).thenThrow(RuntimeException.class);
        assertThrows(RuntimeException.class, () -> objectUnderTest.setValue(input, UUID.randomUUID().toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "new-secret-to-set"
    })
    void testSetValueSuccess(final String input) {
        String newVersionIdToSet = UUID.randomUUID().toString();
        when(secretsSupplier.updateValue(secretId, secretKey, input, newVersionIdToSet)).thenReturn(newVersionIdToSet);
        objectUnderTest.setValue(input, newVersionIdToSet);
        assertThat(objectUnderTest.getValue(), equalTo(input));
    }

}