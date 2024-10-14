package org.opensearch.dataprepper.core.validation;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.validation.PluginError;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class PluginErrorCollectorTest {

    @Test
    void testWithPluginErrors() {
        final PluginErrorCollector objectUnderTest = new PluginErrorCollector();
        final PluginError pluginError1 = mock(PluginError.class);
        final PluginError pluginError2 = mock(PluginError.class);
        objectUnderTest.collectPluginError(pluginError1);
        objectUnderTest.collectPluginError(pluginError2);
        assertThat(objectUnderTest.getPluginErrors().size(), equalTo(2));
    }
}