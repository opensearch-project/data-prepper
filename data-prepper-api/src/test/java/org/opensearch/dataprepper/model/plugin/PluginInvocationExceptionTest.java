/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class PluginInvocationExceptionTest {
    private String message;
    private Throwable cause;

    @BeforeEach
    void setUp() {
        message = UUID.randomUUID().toString();
        cause = mock(Throwable.class);
    }

    private PluginInvocationException createObjectUnderTest() {
        return new PluginInvocationException(message, cause);
    }

    @Test
    void getMessage_returns_message() {
        assertThat(createObjectUnderTest().getMessage(),
                equalTo(message));
    }

    @Test
    void getCause_returns_cause() {
        assertThat(createObjectUnderTest().getCause(),
                equalTo(cause));
    }
}