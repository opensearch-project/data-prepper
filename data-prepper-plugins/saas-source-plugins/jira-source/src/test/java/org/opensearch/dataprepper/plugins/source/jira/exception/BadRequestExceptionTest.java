package org.opensearch.dataprepper.plugins.source.jira.exception;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

public class BadRequestExceptionTest {
    private String message;
    private Throwable throwable;

    @BeforeEach
    void setUp() {
        message = "Bad Request";
        throwable = mock(Throwable.class);
    }

    @Nested
    class MessageOnlyConstructor {
        private BadRequestException createObjectUnderTest() {
            return new BadRequestException(message);
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
        private BadRequestException createObjectUnderTest() {
            return new BadRequestException(message, throwable);
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
