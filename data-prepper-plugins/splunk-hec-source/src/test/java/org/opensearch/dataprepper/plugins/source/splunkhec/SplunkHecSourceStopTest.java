/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.splunkhec.model.HecTokenConfig;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SplunkHecSourceStopTest {

    private static final EventFactory TEST_EVENT_FACTORY = TestEventFactory.getTestEventFactory();

    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private PipelineDescription pipelineDescription;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private Buffer<Record<Event>> buffer;

    @Test
    void stop_without_start_does_not_throw() {
        final SplunkHecSourceConfig config = createConfig(false);
        when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");

        final SplunkHecSource source = new SplunkHecSource(config, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, TEST_EVENT_FACTORY);
        source.stop();

        verifyNoInteractions(buffer);
    }

    @Test
    void stop_after_getHttpService_shuts_down_service() {
        final SplunkHecSourceConfig config = createConfig(true);
        when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");

        final SplunkHecSource source = spy(new SplunkHecSource(config, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, TEST_EVENT_FACTORY));
        final SplunkHecService service = mock(SplunkHecService.class);
        doReturn(service).when(source).createSplunkHecService(anyInt(), any(), any());

        source.getHttpService(8000, buffer, pluginMetrics);
        source.stop();

        verify(service).shutdown();
    }

    @Test
    void areAcknowledgementsEnabled_delegates_to_config() {
        final SplunkHecSourceConfig configEnabled = createConfig(true);
        when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");

        final SplunkHecSource source = new SplunkHecSource(configEnabled, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, TEST_EVENT_FACTORY);
        assertThat(source.areAcknowledgementsEnabled(), is(true));
    }

    private SplunkHecSourceConfig createConfig(final boolean acknowledgements) {
        final SplunkHecSourceConfig config = mock(SplunkHecSourceConfig.class);
        lenient().when(config.getPort()).thenReturn(0);
        lenient().when(config.getPath()).thenReturn("/services/collector");
        lenient().when(config.getAuthentication()).thenReturn(null);
        lenient().when(config.isSsl()).thenReturn(false);
        lenient().when(config.getMaxConnectionCount()).thenReturn(500);
        lenient().when(config.getRequestTimeoutInMillis()).thenReturn(10000);
        lenient().when(config.getThreadCount()).thenReturn(200);
        lenient().when(config.getMaxPendingRequests()).thenReturn(1024);
        lenient().when(config.getBufferTimeoutInMillis()).thenReturn(8000);
        lenient().when(config.hasHealthCheckService()).thenReturn(false);
        lenient().when(config.isAcknowledgements()).thenReturn(acknowledgements);
        lenient().when(config.getAcknowledgementExpiry()).thenReturn(Duration.ofSeconds(300));

        final HecTokenConfig tokenConfig = mock(HecTokenConfig.class);
        lenient().when(tokenConfig.getToken()).thenReturn("test-token");
        lenient().when(tokenConfig.isEnabled()).thenReturn(true);
        lenient().when(tokenConfig.getDefaults()).thenReturn(null);
        lenient().when(config.getTokens()).thenReturn(List.of(tokenConfig));
        lenient().when(config.getDefaultSourcetype()).thenReturn("httpevent");
        lenient().when(config.getRawLineBreaker()).thenReturn("\n");
        lenient().when(config.isFlattenEvent()).thenReturn(true);
        return config;
    }
}
