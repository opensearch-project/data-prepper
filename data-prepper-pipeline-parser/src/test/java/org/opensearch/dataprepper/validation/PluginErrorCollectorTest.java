package org.opensearch.dataprepper.validation;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PluginErrorCollectorTest {

    @Test
    void testWithPluginErrors() {
        final PluginErrorCollector objectUnderTest = new PluginErrorCollector();
        final String testErrorMessage1 = "test error message 1";
        final String testErrorMessage2 = "test error message 2";
        final PluginError pluginError1 = mock(PluginError.class);
        when(pluginError1.getErrorMessage()).thenReturn(testErrorMessage1);
        final PluginError pluginError2 = mock(PluginError.class);
        when(pluginError2.getErrorMessage()).thenReturn(testErrorMessage2);
        objectUnderTest.collectPluginError(pluginError1);
        objectUnderTest.collectPluginError(pluginError2);
        assertThat(objectUnderTest.getPluginErrors().size(), equalTo(2));
        assertThat(objectUnderTest.getAllErrorMessages().size(), equalTo(2));
        assertThat(objectUnderTest.getAllErrorMessages(), contains(testErrorMessage1, testErrorMessage2));
        assertThat(objectUnderTest.getConsolidatedErrorMessage(), equalTo(
                String.format("1. %s\n2. %s", testErrorMessage1, testErrorMessage2)));
    }
}