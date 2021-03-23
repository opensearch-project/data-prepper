package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.sink.elasticsearch.ConnectionConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class OtelTraceGroupPrepperConfigTests {

    @Mock
    private ConnectionConfiguration connectionConfigurationMock;

    @Test
    public void testInitialize() {
        try (MockedStatic<ConnectionConfiguration> connectionConfigurationMockedStatic = Mockito.mockStatic(ConnectionConfiguration.class)) {
            connectionConfigurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(any(PluginSetting.class)))
                    .thenReturn(connectionConfigurationMock);
            PluginSetting testPluginSetting = new PluginSetting("otel_trace_group_prepper", new HashMap<>());
            OtelTraceGroupPrepperConfig otelTraceGroupPrepperConfig = OtelTraceGroupPrepperConfig.buildConfig(testPluginSetting);
            assertEquals(connectionConfigurationMock, otelTraceGroupPrepperConfig.getEsConnectionConfig());
        }
    }
}
