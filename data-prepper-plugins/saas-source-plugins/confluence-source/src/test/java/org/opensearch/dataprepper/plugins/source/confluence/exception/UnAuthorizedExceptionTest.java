/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.exception;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.confluence.exception.UnAuthorizedException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class UnAuthorizedExceptionTest {
    private String message;
    private Throwable throwable;

    @BeforeEach
    void setUp() {
        message = "UnAuthorized Exception";
        throwable = mock(Throwable.class);
    }

    @Nested
    class MessageOnlyConstructor {
        private UnAuthorizedException createObjectUnderTest() {
            return new UnAuthorizedException(message);
        }

        @Test
        void getMessage_returns_message() {
            assertEquals(createObjectUnderTest().getMessage(), message);
        }

        @Test
        void getCause_returns_null() {
            assertNull(createObjectUnderTest().getCause());
        }
    }

    @Nested
    class MessageThrowableConstructor {
        private UnAuthorizedException createObjectUnderTest() {
            return new UnAuthorizedException(message, throwable);
        }

        @Test
        void getMessage_returns_message() {
            assertEquals(createObjectUnderTest().getMessage(), message);
        }

        @Test
        void getCause_returns_throwable() {
            assertEquals(createObjectUnderTest().getCause(), throwable);
        }
    }
}
