package org.opensearch.dataprepper.core.validation;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.validation.PluginError;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginErrorTest {
    private static final String TEST_PIPELINE_NAME = "test-pipeline";
    private static final String TEST_COMPONENT_TYPE = "test-plugin-type";
    private static final String TEST_PLUGIN_NAME = "test-plugin";
    private static final String TEST_ERROR_MESSAGE = "test error message";
    private static final String TEST_CAUSE_ERROR_MESSAGE = "test cause error message";

    @Test
    void testGetErrorMessageWithPipelineName() {
        final Exception exception = mock(Exception.class);
        when(exception.getMessage()).thenReturn(TEST_ERROR_MESSAGE);
        final PluginError pluginError = PluginError.builder()
                .pipelineName(TEST_PIPELINE_NAME)
                .componentType(TEST_COMPONENT_TYPE)
                .pluginName(TEST_PLUGIN_NAME)
                .exception(exception)
                .build();
        assertThat(pluginError.getErrorMessage(), equalTo(
                "test-pipeline:test-plugin-type:test-plugin: caused by: test error message"));
    }

    @Test
    void testGetErrorMessageWithoutPipelineName() {
        final Exception exception = mock(Exception.class);
        when(exception.getMessage()).thenReturn(TEST_ERROR_MESSAGE);
        final PluginError pluginError = PluginError.builder()
                .pluginName(TEST_PLUGIN_NAME)
                .componentType(TEST_COMPONENT_TYPE)
                .exception(exception)
                .build();
        assertThat(pluginError.getErrorMessage(), equalTo(
                "test-plugin-type:test-plugin: caused by: test error message"));
    }

    @Test
    void testGetErrorMessageWithCause() {
        final Exception exception = mock(Exception.class);
        final Exception cause = mock(Exception.class);
        when(exception.getMessage()).thenReturn(TEST_ERROR_MESSAGE);
        when(cause.getMessage()).thenReturn(TEST_CAUSE_ERROR_MESSAGE);
        when(exception.getCause()).thenReturn(cause);
        final PluginError pluginError = PluginError.builder()
                .pluginName(TEST_PLUGIN_NAME)
                .exception(exception)
                .build();
        assertThat(pluginError.getErrorMessage(), equalTo(
                "test-plugin: caused by: test error message caused by: test cause error message"));
    }
}