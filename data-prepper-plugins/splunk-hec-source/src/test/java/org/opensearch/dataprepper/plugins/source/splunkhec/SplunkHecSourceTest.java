/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.http.BaseHttpService;
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

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SplunkHecSourceTest {

    private static final EventFactory TEST_EVENT_FACTORY = TestEventFactory.getTestEventFactory();

    @Mock
    private SplunkHecSourceConfig sourceConfig;

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

    private SplunkHecSource splunkHecSource;

    @BeforeEach
    void setUp() {
        lenient().when(pipelineDescription.getPipelineName()).thenReturn("test-pipeline");
        lenient().when(sourceConfig.getPort()).thenReturn(8088);
        lenient().when(sourceConfig.getPath()).thenReturn("/services/collector");
        lenient().when(sourceConfig.getAuthentication()).thenReturn(null);
        lenient().when(sourceConfig.isSsl()).thenReturn(false);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10000);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(200);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        lenient().when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(8000);
        lenient().when(sourceConfig.hasHealthCheckService()).thenReturn(true);
        lenient().when(sourceConfig.isAcknowledgements()).thenReturn(false);

        final HecTokenConfig tokenConfig = mock(HecTokenConfig.class);
        lenient().when(tokenConfig.getToken()).thenReturn("test-token");
        lenient().when(tokenConfig.isEnabled()).thenReturn(true);
        lenient().when(sourceConfig.getTokens()).thenReturn(List.of(tokenConfig));
        lenient().when(sourceConfig.getDefaultSourcetype()).thenReturn("httpevent");
        lenient().when(sourceConfig.getRawLineBreaker()).thenReturn("\n");
        lenient().when(sourceConfig.isFlattenEvent()).thenReturn(true);

        splunkHecSource = new SplunkHecSource(sourceConfig, pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager, TEST_EVENT_FACTORY);
    }

    @Test
    void constructor_with_null_sourceConfig_throws_NullPointerException() {
        assertThrows(NullPointerException.class, () ->
                new SplunkHecSource(null, pluginMetrics, pluginFactory, pipelineDescription, acknowledgementSetManager, TEST_EVENT_FACTORY));
    }

    @Test
    void constructor_with_null_acknowledgementSetManager_throws_NullPointerException() {
        assertThrows(NullPointerException.class, () ->
                new SplunkHecSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription, null, TEST_EVENT_FACTORY));
    }

    @Test
    void getHttpService_returns_SplunkHecService() {
        final BaseHttpService service = splunkHecSource.getHttpService(8000, buffer, pluginMetrics);
        assertThat(service, notNullValue());
        assertThat(service, instanceOf(SplunkHecService.class));
    }

    @Test
    void areAcknowledgementsEnabled_returns_false_when_disabled() {
        when(sourceConfig.isAcknowledgements()).thenReturn(false);
        assertThat(splunkHecSource.areAcknowledgementsEnabled(), is(false));
    }

    @Test
    void areAcknowledgementsEnabled_returns_true_when_enabled() {
        when(sourceConfig.isAcknowledgements()).thenReturn(true);
        assertThat(splunkHecSource.areAcknowledgementsEnabled(), is(true));
    }
}
