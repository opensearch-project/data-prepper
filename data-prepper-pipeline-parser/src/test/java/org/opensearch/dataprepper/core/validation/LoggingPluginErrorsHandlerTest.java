package org.opensearch.dataprepper.core.validation;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.core.validation.LoggingPluginErrorsHandler;
import org.opensearch.dataprepper.core.validation.PluginError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LoggingPluginErrorsHandlerTest {
    @Test
    void testHandleErrors() {
        final Logger mockLogger = mock(Logger.class);

        try (final MockedStatic<LoggerFactory> mockedLoggerFactory = mockStatic(LoggerFactory.class)) {
            mockedLoggerFactory.when(() -> LoggerFactory.getLogger(LoggingPluginErrorsHandler.class))
                    .thenReturn(mockLogger);
            final LoggingPluginErrorsHandler loggingPluginErrorsHandler = new LoggingPluginErrorsHandler();
            final PluginError error1 = mock(PluginError.class);
            final PluginError error2 = mock(PluginError.class);
            when(error1.getErrorMessage()).thenReturn("Error 1 occurred");
            when(error2.getErrorMessage()).thenReturn("Error 2 occurred");
            final Collection<PluginError> pluginErrors = Arrays.asList(error1, error2);
            loggingPluginErrorsHandler.handleErrors(pluginErrors);
            final String expectedMessage = "1. Error 1 occurred\n2. Error 2 occurred";
            verify(mockLogger).error(expectedMessage);
        }
    }
}