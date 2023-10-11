package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SecretsRefreshJobTest {
    private static final String TEST_SECRET_CONFIG_ID = "test_secret_config_id";
    @Mock
    private SecretsSupplier secretsSupplier;

    @Mock
    private PluginConfigPublisher pluginConfigPublisher;

    private SecretsRefreshJob objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new SecretsRefreshJob(TEST_SECRET_CONFIG_ID, secretsSupplier, pluginConfigPublisher);
    }

    @Test
    void testRun() {
        objectUnderTest.run();

        verify(secretsSupplier).refresh(TEST_SECRET_CONFIG_ID);
        verify(pluginConfigPublisher).notifyAllPluginConfigObservable();
    }
}