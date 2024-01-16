/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class DefaultGeoIpConfigSupplierTest {
    @Mock
    private GeoIpServiceConfig geoIpServiceConfig;

    private DefaultGeoIpConfigSupplier createObjectUnderTest() {
        return new DefaultGeoIpConfigSupplier(geoIpServiceConfig);
    }

    @Test
    void getGeoIPProcessorService_returnsNull_whenGeoIpServiceConfigIsNull() {
        assertThat(createObjectUnderTest().getGeoIPProcessorService(), equalTo(null));
    }

}