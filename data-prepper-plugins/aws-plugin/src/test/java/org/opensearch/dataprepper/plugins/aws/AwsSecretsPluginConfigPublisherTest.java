package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.plugin.PluginConfigurationObservable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AwsSecretsPluginConfigPublisherTest {

    private AwsSecretsPluginConfigPublisher objectUnderTest;

    @Test
    void testAddPluginConfigurationObservableAndThenNotifyAll() {
        final PluginConfigurationObservable pluginConfigurationObservable1 = mock(PluginConfigurationObservable.class);
        final PluginConfigurationObservable pluginConfigurationObservable2 = mock(PluginConfigurationObservable.class);
        objectUnderTest = new AwsSecretsPluginConfigPublisher();
        objectUnderTest.addPluginConfigurationObservable(pluginConfigurationObservable1);
        objectUnderTest.addPluginConfigurationObservable(pluginConfigurationObservable2);
        objectUnderTest.notifyAllPluginConfigurationObservable();
        verify(pluginConfigurationObservable1).update();
        verify(pluginConfigurationObservable2).update();
    }
}