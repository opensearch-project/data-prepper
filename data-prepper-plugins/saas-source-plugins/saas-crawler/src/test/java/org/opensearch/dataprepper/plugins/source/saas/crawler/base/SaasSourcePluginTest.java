package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SaasSourcePluginTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private Crawler crawler;

    @Mock
    private SaasPluginExecutorServiceProvider executorServiceProvider;

    @Mock
    private ExecutorService executorService;

    @Mock
    private SaasSourceConfig sourceConfig;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private SaasSourcePlugin saasSourcePlugin;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @BeforeEach
    void setUp() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        saasSourcePlugin = new SaasSourcePlugin(pluginMetrics, sourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
    }

    @Test
    void pluginConstructorTest() {
        assertNotNull(saasSourcePlugin);
        verify(executorServiceProvider).get();
    }

    @Test
    void startTest() {
        saasSourcePlugin.setEnhancedSourceCoordinator(sourceCoordinator);
        saasSourcePlugin.start(buffer);

    }

    @Test
    void testStop() {
        saasSourcePlugin.stop();
        verify(executorService).shutdownNow();
    }



}
