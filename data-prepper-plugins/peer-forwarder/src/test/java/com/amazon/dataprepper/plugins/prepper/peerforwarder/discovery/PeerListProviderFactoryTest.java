/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.PeerForwarderConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerListProviderFactoryTest {
    private static final String PLUGIN_NAME = "PLUGIN_NAME";
    private static final String PIPELINE_NAME = "pipelineName";

    private PluginSetting pluginSetting;

    private PeerListProviderFactory factory;

    @BeforeEach
    void setup() {
        factory = new PeerListProviderFactory();
        pluginSetting = new PluginSetting(PLUGIN_NAME, new HashMap<>()) {{
            setPipelineName(PIPELINE_NAME);
        }};
    }

    @Test
    void createProvider_throws_when_no_DiscoveryMode_is_provided() {
        assertThrows(NullPointerException.class,
                () -> factory.createProvider(pluginSetting));
    }

    @Test
    void createProvider_throws_for_undefined_DiscoveryMode() {
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, "GARBAGE");

        assertThrows(IllegalArgumentException.class,
                () -> factory.createProvider(pluginSetting));
    }

    @ParameterizedTest
    @EnumSource(DiscoveryMode.class)
    void createProvider_returns_correct_provider_for_all_DiscoveryModes(final DiscoveryMode discoveryMode) {
        final String discoveryModeString = discoveryMode.toString();
        pluginSetting.getSettings().put(PeerForwarderConfig.DISCOVERY_MODE, discoveryModeString.toLowerCase());

        final PeerListProvider expectedProvider = mock(PeerListProvider.class);
        final PeerListProvider actualProvider;

        try (final MockedStatic<DiscoveryMode> enumMock = Mockito.mockStatic(DiscoveryMode.class)) {
            final DiscoveryMode mockedModeEnum = mock(DiscoveryMode.class);
            enumMock.when(() -> DiscoveryMode.valueOf(discoveryModeString)).thenReturn(mockedModeEnum);

            when(mockedModeEnum.create(eq(pluginSetting), any(PluginMetrics.class))).thenReturn(expectedProvider);

            actualProvider = factory.createProvider(pluginSetting);
        }

        assertThat(actualProvider, sameInstance(expectedProvider));
    }

}
