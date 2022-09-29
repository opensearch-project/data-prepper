/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class OTelTraceGroupProcessorConfigTests {

    @Mock
    private ConnectionConfiguration connectionConfigurationMock;

    @Test
    public void testInitialize() {
        try (MockedStatic<ConnectionConfiguration> connectionConfigurationMockedStatic = Mockito.mockStatic(ConnectionConfiguration.class)) {
            connectionConfigurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(any(PluginSetting.class)))
                    .thenReturn(connectionConfigurationMock);
            PluginSetting testPluginSetting = new PluginSetting("otel_trace_group", new HashMap<>());
            OTelTraceGroupProcessorConfig otelTraceGroupProcessorConfig = OTelTraceGroupProcessorConfig.buildConfig(testPluginSetting);
            assertEquals(connectionConfigurationMock, otelTraceGroupProcessorConfig.getEsConnectionConfig());
        }
    }
}
