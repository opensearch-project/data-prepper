package org.opensearch.dataprepper.plugins.source.crowdstrike;

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
import org.opensearch.dataprepper.plugins.source.crowdstrike.rest.CrowdStrikeAuthClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CrowdStrikeSourceTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private CrowdStrikeSourceConfig crowdStrikeSourceConfig;
    @Mock
    private CrowdStrikeAuthClient authClient;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private Crawler crawler;
    @Mock
    private PluginExecutorServiceProvider executorServiceProvider;
    @Mock
    private ExecutorService executorService;
    @Mock
    private Buffer<Record<Event>> buffer;

    private CrowdStrikeSource crowdStrikeSource;

    @BeforeEach
    void setUp() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        crowdStrikeSource = new CrowdStrikeSource(
                crowdStrikeSourceConfig,
                pluginMetrics,
                pluginFactory,
                acknowledgementSetManager,
                authClient,
                crawler,
                executorServiceProvider
        );
    }

    @Test
    void testInitialization() {
        assertNotNull(crowdStrikeSource);
    }

    @Test
    void testStart_ShouldCallInitCredentialsAndSubmitTask() {
        crowdStrikeSource.start(buffer);
        verify(authClient, times(1)).initCredentials();
    }

    @Test
    void testStop_ShouldCallSuperStop() {
        crowdStrikeSource.start(buffer);
        crowdStrikeSource.stop();
        verify(executorService).shutdownNow();
    }

    @Test
    void testStop_WhenNotStarted() {
        crowdStrikeSource.stop();
        verify(executorService, never()).shutdown();
    }
}
