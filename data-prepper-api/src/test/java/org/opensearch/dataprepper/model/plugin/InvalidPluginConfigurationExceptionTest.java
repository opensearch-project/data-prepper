/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class InvalidPluginConfigurationExceptionTest {
    @Mock
    private Throwable cause;

    private String message;

    @BeforeEach
    void setUp() {
        message = UUID.randomUUID().toString();
    }

    private InvalidPluginConfigurationException createObjectUnderTest() {
        return new InvalidPluginConfigurationException(message);
    }

    @Test
    void getMessage_returns_message() {
        assertThat(createObjectUnderTest().getMessage(),
                equalTo(message));
    }

    @Test
    void getCause_returns_cause() {
        final InvalidPluginConfigurationException objectUnderTest = new InvalidPluginConfigurationException(
                message, cause);
        assertThat(objectUnderTest.getCause(), equalTo(cause));
    }
}