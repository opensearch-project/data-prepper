package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AwsSecretsPluginConfigPublisherTest {

    private AwsSecretsPluginConfigPublisher objectUnderTest;

    @Test
    void testAddPluginConfigurationObservableAndThenNotifyAll() {
        final PluginConfigObservable pluginConfigObservable1 = mock(PluginConfigObservable.class);
        final PluginConfigObservable pluginConfigObservable2 = mock(PluginConfigObservable.class);
        objectUnderTest = new AwsSecretsPluginConfigPublisher();
        objectUnderTest.addPluginConfigObservable(pluginConfigObservable1);
        objectUnderTest.addPluginConfigObservable(pluginConfigObservable2);
        objectUnderTest.notifyAllPluginConfigObservable();
        verify(pluginConfigObservable1).update();
        verify(pluginConfigObservable2).update();
    }
}