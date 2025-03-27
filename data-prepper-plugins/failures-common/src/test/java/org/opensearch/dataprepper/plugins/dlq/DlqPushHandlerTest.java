/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.dlq;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class DlqPushHandlerTest {
    static final String METRICS_PREFIX="prefix";
    static final String TEST_NAME="name";
    static final String TEST_PIPELINE_NAME="pipelineName";

    @Mock
    PluginFactory pluginFactory;
    @Mock
    PluginSetting pluginSetting;
    @Mock
    PluginMetrics pluginMetrics;
    @Mock
    private Counter counter;
    @Mock
    private DlqProvider dlqProvider;
    @Mock
    private DlqWriter dlqWriter;

    private DlqPushHandler dlqPushHandler;
    private PluginModel dlqConfig;
    private AtomicInteger count;


    @BeforeEach
    void setup() {
        count = new AtomicInteger(0);
        pluginFactory = mock(PluginFactory.class);
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        dlqProvider = mock(DlqProvider.class);
        dlqWriter = mock(DlqWriter.class);
        counter = mock(Counter.class);
        when(pluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        when(pluginSetting.getName()).thenReturn(TEST_NAME);
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
        when(dlqProvider.getDlqWriter(any())).thenReturn(Optional.of(dlqWriter));
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(dlqProvider);
        dlqConfig = mock(PluginModel.class);
        when(dlqConfig.getPluginName()).thenReturn(TEST_NAME);
        when(dlqConfig.getPluginSettings()).thenReturn(new HashMap<String, Object>());
    }

    DlqPushHandler createObjectUnderTest(final String region, final String role) {
        return new DlqPushHandler(pluginFactory, pluginSetting, pluginMetrics, dlqConfig, region, role, METRICS_PREFIX);
    }

    @Test
    void testDlqPushHandler_basic() {
        final String region = UUID.randomUUID().toString();
        final String role = UUID.randomUUID().toString();
        dlqPushHandler = createObjectUnderTest(region, role);
        PluginSetting pSetting = dlqPushHandler.getPluginSetting();
        assertThat(pSetting.getName(), equalTo(TEST_NAME));
        assertThat(pSetting.getPipelineName(), equalTo(TEST_PIPELINE_NAME));
        assertThat(pSetting.getAttributeFromSettings(DlqPushHandler.REGION), equalTo(region));
        assertThat(pSetting.getAttributeFromSettings(DlqPushHandler.STS_ROLE_ARN), equalTo(role));
        assertThat(dlqPushHandler.getDlqSuccessCounter(), equalTo(0.0d));
    }

    @Test
    void testDlqPushHandler_withPerform() throws Exception {
        doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            count.addAndGet(v);
            return null;
        }).when(counter).increment(any(Double.class));
        final String region = UUID.randomUUID().toString();
        final String role = UUID.randomUUID().toString();
        dlqPushHandler = createObjectUnderTest(region, role);
        PluginSetting pSetting = dlqPushHandler.getPluginSetting();
        assertThat(pSetting.getName(), equalTo(TEST_NAME));
        assertThat(pSetting.getPipelineName(), equalTo(TEST_PIPELINE_NAME));
        assertThat(pSetting.getAttributeFromSettings(DlqPushHandler.REGION), equalTo(region));
        assertThat(pSetting.getAttributeFromSettings(DlqPushHandler.STS_ROLE_ARN), equalTo(role));
        assertThat(dlqPushHandler.getDlqSuccessCounter(), equalTo(0.0d));
        DlqObject dlqObject = mock(DlqObject.class);
        List<DlqObject> dlqObjects = List.of(dlqObject);
        doNothing().when(dlqWriter).write(any(List.class), anyString(), anyString());
        dlqPushHandler.perform(dlqObjects);
        when(counter.count()).thenReturn(Double.valueOf(count.get()));
        assertThat(dlqPushHandler.getDlqSuccessCounter(), equalTo(1.0d));
    }

    @Test
    void testDlqPushHandler_withPerform_withDLQFailure() throws Exception {
        doAnswer((a)-> {
            int v = (int)(double)(a.getArgument(0));
            count.addAndGet(v);
            return null;
        }).when(counter).increment(any(Double.class));
        final String region = UUID.randomUUID().toString();
        final String role = UUID.randomUUID().toString();
        dlqPushHandler = createObjectUnderTest(region, role);
        PluginSetting pSetting = dlqPushHandler.getPluginSetting();
        assertThat(pSetting.getName(), equalTo(TEST_NAME));
        assertThat(pSetting.getPipelineName(), equalTo(TEST_PIPELINE_NAME));
        assertThat(pSetting.getAttributeFromSettings(DlqPushHandler.REGION), equalTo(region));
        assertThat(pSetting.getAttributeFromSettings(DlqPushHandler.STS_ROLE_ARN), equalTo(role));
        assertThat(dlqPushHandler.getDlqSuccessCounter(), equalTo(0.0d));
        DlqObject dlqObject = mock(DlqObject.class);
        List<DlqObject> dlqObjects = List.of(dlqObject);
        doThrow(new RuntimeException("failed")).when(dlqWriter).write(any(List.class), anyString(), anyString());
        dlqPushHandler.perform(dlqObjects);
        when(counter.count()).thenReturn(Double.valueOf(count.get()));
        assertThat(dlqPushHandler.getDlqFailedCounter(), equalTo(1.0d));
    }
}
