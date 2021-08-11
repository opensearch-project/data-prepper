package com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.PeerForwarderConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class AwsCloudMapPeerListProvider_CreateTest {

    private static final String PLUGIN_NAME = "PLUGIN_NAME";
    private static final String ENDPOINT = "ENDPOINT";
    private static final String PIPELINE_NAME = "pipelineName";

    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;

    @BeforeEach
    void setUp() {
        pluginSetting = new PluginSetting(PLUGIN_NAME, new HashMap<>()) {{
            setPipelineName(PIPELINE_NAME);
        }};

        pluginMetrics = mock(PluginMetrics.class);

        pluginSetting.getSettings().put(PeerForwarderConfig.DOMAIN_NAME, ENDPOINT);
        pluginSetting.getSettings().put(PeerForwarderConfig.AWS_CLOUD_MAP_NAMESPACE_NAME, UUID.randomUUID().toString());
        pluginSetting.getSettings().put(PeerForwarderConfig.AWS_CLOUD_MAP_SERVICE_NAME, UUID.randomUUID().toString());
        pluginSetting.getSettings().put(PeerForwarderConfig.AWS_REGION, "us-east-1");

    }

    @Test
    void createPeerListProvider_with_valid_configurations() {
        final PeerListProvider result = AwsCloudMapPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics);

        assertThat(result, instanceOf(AwsCloudMapPeerListProvider.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            PeerForwarderConfig.AWS_CLOUD_MAP_NAMESPACE_NAME,
            PeerForwarderConfig.AWS_CLOUD_MAP_SERVICE_NAME,
            PeerForwarderConfig.AWS_REGION
    })
    void createPeerListProvider_with_missing_required_property(final String propertyToRemove) {
        pluginSetting.getSettings().remove(propertyToRemove);

        assertThrows(NullPointerException.class,
                () -> AwsCloudMapPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics));

    }

    @ParameterizedTest
    @ArgumentsSource(AllRegionsArgumentProvider.class)
    void createPeerListProvider_with_all_current_regions(final Region region) {
        pluginSetting.getSettings().put(PeerForwarderConfig.AWS_REGION, region.toString());

        final PeerListProvider result = AwsCloudMapPeerListProvider.createPeerListProvider(pluginSetting, pluginMetrics);

        assertThat(result, instanceOf(AwsCloudMapPeerListProvider.class));
    }

    static class AllRegionsArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Region.regions().stream().map(Arguments::of);
        }
    }
}