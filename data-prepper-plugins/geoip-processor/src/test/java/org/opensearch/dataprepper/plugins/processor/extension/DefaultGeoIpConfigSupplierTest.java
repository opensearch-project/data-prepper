/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mockConstruction;

@ExtendWith(MockitoExtension.class)
class DefaultGeoIpConfigSupplierTest {
    @Mock
    private GeoIpServiceConfig geoIpServiceConfig;

    private DefaultGeoIpConfigSupplier createObjectUnderTest() {
        return new DefaultGeoIpConfigSupplier(geoIpServiceConfig);
    }

    @Test
    void getGeoIpProcessorService_returns_geoIPProcessorService() {
        try (final MockedConstruction<GeoIPProcessorService> mockedConstruction =
                mockConstruction(GeoIPProcessorService.class)) {
            final DefaultGeoIpConfigSupplier objectUnderTest = createObjectUnderTest();
            final GeoIPProcessorService geoIPProcessorService = objectUnderTest.getGeoIPProcessorService();

            assertThat(mockedConstruction.constructed().size(), equalTo(1));
            assertThat(geoIPProcessorService, instanceOf(GeoIPProcessorService.class));
            assertThat(geoIPProcessorService, equalTo(mockedConstruction.constructed().get(0)));
        }
    }
}
