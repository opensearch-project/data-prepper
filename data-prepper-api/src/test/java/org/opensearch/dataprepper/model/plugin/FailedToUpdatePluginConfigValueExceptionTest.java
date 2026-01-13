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

public class FailedToUpdatePluginConfigValueExceptionTest extends RuntimeException {
    private String message;

    @BeforeEach
    void setUp() {
        message = UUID.randomUUID().toString();
    }

    @Test
    void testGetMessage_should_return_correct_message() {
        FailedToUpdatePluginConfigValueException failedToUpdateSecretException = new FailedToUpdatePluginConfigValueException(message);
        assertThat(failedToUpdateSecretException.getMessage(), equalTo(message));
    }

    @Test
    void testGetMessage_should_return_correct_message_with_throwable() {
        RuntimeException cause = new RuntimeException("testException");
        FailedToUpdatePluginConfigValueException failedToUpdateSecretException = new FailedToUpdatePluginConfigValueException(message, cause);
        assertThat(failedToUpdateSecretException.getMessage(), equalTo(message));
        assertThat(failedToUpdateSecretException.getCause(), equalTo(cause));
    }

}
