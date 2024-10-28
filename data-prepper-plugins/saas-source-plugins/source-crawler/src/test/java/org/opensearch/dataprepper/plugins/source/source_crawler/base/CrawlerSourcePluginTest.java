package org.opensearch.dataprepper.plugins.source.source_crawler.base;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.LeaderScheduler;

import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
public class CrawlerSourcePluginTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private Crawler crawler;

    @Mock
    private PluginExecutorServiceProvider executorServiceProvider;

    @Mock
    private ExecutorService executorService;

    @Mock
    private CrawlerSourceConfig sourceConfig;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;


    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    private testCrawlerSourcePlugin saasSourcePlugin;

    @BeforeEach
    void setUp() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        saasSourcePlugin = new testCrawlerSourcePlugin(pluginMetrics, sourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
    }

    @Test
    void pluginConstructorTest() {
        assertNotNull(saasSourcePlugin);
        verify(executorServiceProvider).get();
    }

    @Test
    void testSetEnhancedSourceCoordinator() {
        saasSourcePlugin.setEnhancedSourceCoordinator(sourceCoordinator);
        verify(sourceCoordinator).initialize();
    }

    @Test
    void areAcknowledgementsEnabledTest() {
        assertFalse(saasSourcePlugin.areAcknowledgementsEnabled());
    }

    @Test
    void startTest() {
        saasSourcePlugin.setEnhancedSourceCoordinator(sourceCoordinator);
        saasSourcePlugin.start(buffer);
        assertFalse(executorService.isShutdown());
        assertFalse(executorService.isTerminated());
    }

    @Test
    void testExecutorServiceSchedulersSubmitted() {
        saasSourcePlugin.setEnhancedSourceCoordinator(sourceCoordinator);
        saasSourcePlugin.start(buffer);
        verify(executorService, times(1)).submit(any(LeaderScheduler.class));
        verify(executorService, times(CrawlerSourceConfig.DEFAULT_NUMBER_OF_WORKERS))
                .submit(any(Thread.class));
    }

    @Test
    void testStop() {
        saasSourcePlugin.stop();
        verify(executorService).shutdownNow();
    }

    @Test
    void testGetPartitionFactory() {
        Function<SourcePartitionStoreItem, EnhancedSourcePartition> factory = saasSourcePlugin.getPartitionFactory();
        assertNotNull(factory);

    }

    @Test
    void testGetDecoder() {
        ByteDecoder decoder = saasSourcePlugin.getDecoder();
        assertNull(decoder);

    }

    @Nested
    public class testCrawlerSourcePlugin extends CrawlerSourcePlugin {
        public testCrawlerSourcePlugin(final PluginMetrics pluginMetrics,
                                       final CrawlerSourceConfig sourceConfig,
                                       final PluginFactory pluginFactory,
                                       final AcknowledgementSetManager acknowledgementSetManager,
                                       final Crawler crawler,
                                       final PluginExecutorServiceProvider executorServiceProvider) {
            super("TestcasePlugin", pluginMetrics, sourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
        }
    }


}