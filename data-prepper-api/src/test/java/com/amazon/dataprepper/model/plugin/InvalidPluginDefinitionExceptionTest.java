package com.amazon.dataprepper.model.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class InvalidPluginDefinitionExceptionTest {
    private String message;
    private Throwable cause;

    @BeforeEach
    void setUp() {
        message = UUID.randomUUID().toString();
        cause = mock(Throwable.class);
    }

    private InvalidPluginDefinitionException createObjectUnderTest() {
        return new InvalidPluginDefinitionException(message, cause);
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