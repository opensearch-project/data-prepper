package org.opensearch.dataprepper.plugins.source.saas.jira.exception;

import net.bytebuddy.implementation.bytecode.Throw;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Nested;


import static org.hamcrest.MatcherAssert.assertThat;
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
