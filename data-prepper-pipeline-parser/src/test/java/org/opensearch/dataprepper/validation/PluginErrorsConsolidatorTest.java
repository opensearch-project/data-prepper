package org.opensearch.dataprepper.validation;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginErrorsConsolidatorTest {

    @Test
    void testWithPluginErrors() {
        final PluginErrorsConsolidator objectUnderTest = new PluginErrorsConsolidator();
        final String testErrorMessage1 = "test error message 1";
        final String testErrorMessage2 = "test error message 2";
        final PluginError pluginError1 = mock(PluginError.class);
        when(pluginError1.getErrorMessage()).thenReturn(testErrorMessage1);
        final PluginError pluginError2 = mock(PluginError.class);
        when(pluginError2.getErrorMessage()).thenReturn(testErrorMessage2);
        final String consolidatedErrorMessage = objectUnderTest.consolidatedErrorMessage(
                List.of(pluginError1, pluginError2));
        assertThat(consolidatedErrorMessage, equalTo(
                String.format("1. %s\n2. %s", testErrorMessage1, testErrorMessage2)));
    }
}