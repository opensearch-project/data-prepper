/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

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
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.AuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.DimensionalTimeSliceCrawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class Office365SourceTest {
    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Office365SourceConfig office365SourceConfig;

    @Mock
    private Office365AuthenticationInterface office365AuthProvider;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private DimensionalTimeSliceCrawler crawler;

    @Mock
    private PluginExecutorServiceProvider executorServiceProvider;

    @Mock
    private ExecutorService executorService;

    @Mock
    private AuthenticationConfiguration authenticationConfiguration;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private Office365Service office365Service;


    @Test
    void initialization() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        Office365Source source = new Office365Source(pluginMetrics, office365SourceConfig,
                office365AuthProvider, pluginFactory, acknowledgementSetManager, crawler,
                executorServiceProvider, office365Service);
        assertNotNull(source);
    }

    @Test
    void testStart() {
        when(executorServiceProvider.get()).thenReturn(executorService);

        Office365Source source = new Office365Source(pluginMetrics, office365SourceConfig,
                office365AuthProvider, pluginFactory, acknowledgementSetManager, crawler,
                executorServiceProvider, office365Service);

        source.setEnhancedSourceCoordinator(sourceCoordinator);
        source.start(buffer);

        verify(office365AuthProvider).initCredentials();
        verify(office365Service).initializeSubscriptions();
        verify(executorService, atLeast(1)).submit(any(Runnable.class));
    }

    @Test
    void testStartWithAuthenticationFailure() {
        when(executorServiceProvider.get()).thenReturn(executorService);

        Office365Source source = new Office365Source(pluginMetrics, office365SourceConfig,
                office365AuthProvider, pluginFactory, acknowledgementSetManager, crawler,
                executorServiceProvider, office365Service);

        doThrow(new RuntimeException("Authentication failed"))
                .when(office365AuthProvider).initCredentials();

        source.setEnhancedSourceCoordinator(sourceCoordinator);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> source.start(buffer));
        assertEquals("Failed to start Office365 Source Plugin", exception.getMessage());
    }



    @Test
    void testStop() {
        when(executorServiceProvider.get()).thenReturn(executorService);

        Office365Source source = new Office365Source(pluginMetrics, office365SourceConfig,
                office365AuthProvider, pluginFactory, acknowledgementSetManager, crawler,
                executorServiceProvider, office365Service);

        source.setEnhancedSourceCoordinator(sourceCoordinator);
        source.start(buffer);
        source.stop();

        verify(executorService).shutdownNow();
    }

    @Test
    void testStop_WhenNotStarted() {
        when(executorServiceProvider.get()).thenReturn(executorService);

        Office365Source source = new Office365Source(pluginMetrics, office365SourceConfig,
                office365AuthProvider, pluginFactory, acknowledgementSetManager, crawler,
                executorServiceProvider, office365Service);

        source.stop();

        verify(executorService, never()).shutdown();
    }
}
