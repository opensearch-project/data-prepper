/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.exception;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class UnauthorizedExceptionTest {
    private String message;
    private Throwable throwable;

    @BeforeEach
    void setUp() {
        message = "UnAuthorized Exception";
        throwable = mock(Throwable.class);
    }

    @Nested
    class MessageOnlyConstructor {
        private UnauthorizedException createObjectUnderTest() {
            return new UnauthorizedException(message);
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
        private UnauthorizedException createObjectUnderTest() {
            return new UnauthorizedException(message, throwable);
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
