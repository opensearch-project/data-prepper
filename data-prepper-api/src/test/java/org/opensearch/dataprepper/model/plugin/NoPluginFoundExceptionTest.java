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

class NoPluginFoundExceptionTest {
    private String message;

    @BeforeEach
    void setUp() {
        message = UUID.randomUUID().toString();
    }

    private NoPluginFoundException createObjectUnderTest() {
        return new NoPluginFoundException(message);
    }

    @Test
    void getMessage_returns_message() {
        assertThat(createObjectUnderTest().getMessage(),
                equalTo(message));
    }
}