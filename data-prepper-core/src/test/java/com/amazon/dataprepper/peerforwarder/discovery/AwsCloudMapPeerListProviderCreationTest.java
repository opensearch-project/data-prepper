/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder.discovery;

import com.amazon.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import software.amazon.awssdk.regions.Region;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsCloudMapPeerListProviderCreationTest {

    private PeerForwarderConfiguration peerForwarderConfiguration;

    @BeforeEach
    void setUp() {
        peerForwarderConfiguration = mock(PeerForwarderConfiguration.class);

        when(peerForwarderConfiguration.getAwsCloudMapNamespaceName()).thenReturn(UUID.randomUUID().toString());
        when(peerForwarderConfiguration.getAwsCloudMapServiceName()).thenReturn(UUID.randomUUID().toString());
        when(peerForwarderConfiguration.getAwsRegion()).thenReturn("us-east-1");
        when(peerForwarderConfiguration.getAwsCloudMapQueryParameters()).thenReturn(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    }

    @Test
    void createPeerListProvider_with_valid_configurations() {
        final PeerListProvider result = AwsCloudMapPeerListProvider.createPeerListProvider(peerForwarderConfiguration);

        assertThat(result, instanceOf(AwsCloudMapPeerListProvider.class));
    }

    @Test
    void createPeerListProvider_with_missing_required_service_name() {
        when(peerForwarderConfiguration.getAwsCloudMapServiceName()).thenReturn(null);

        assertThrows(NullPointerException.class,
                () -> AwsCloudMapPeerListProvider.createPeerListProvider(peerForwarderConfiguration));

    }

    @Test
    void createPeerListProvider_with_missing_required_namespace_name() {
        when(peerForwarderConfiguration.getAwsCloudMapNamespaceName()).thenReturn(null);

        assertThrows(NullPointerException.class,
                () -> AwsCloudMapPeerListProvider.createPeerListProvider(peerForwarderConfiguration));

    }

    @Test
    void createPeerListProvider_with_missing_required_region() {
        when(peerForwarderConfiguration.getAwsRegion()).thenReturn(null);

        assertThrows(NullPointerException.class,
                () -> AwsCloudMapPeerListProvider.createPeerListProvider(peerForwarderConfiguration));

    }

    @Test
    void createPeerListProvider_with_missing_required_query_parameters() {
        when(peerForwarderConfiguration.getAwsCloudMapQueryParameters()).thenReturn(null);

        assertThrows(NullPointerException.class,
                () -> AwsCloudMapPeerListProvider.createPeerListProvider(peerForwarderConfiguration));

    }

    @ParameterizedTest
    @ArgumentsSource(AllRegionsArgumentProvider.class)
    void createPeerListProvider_with_all_current_regions(final Region region) {
        when(peerForwarderConfiguration.getAwsRegion()).thenReturn(String.valueOf(region));

        final PeerListProvider result = AwsCloudMapPeerListProvider.createPeerListProvider(peerForwarderConfiguration);

        assertThat(result, instanceOf(AwsCloudMapPeerListProvider.class));
    }

    static class AllRegionsArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Region.regions().stream().map(Arguments::of);
        }
    }
}