/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.GeoIPDatabaseManager;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorServiceTest {
    @Mock
    private GeoIPDatabaseReader geoIPDatabaseReader;
    @Mock
    private MaxMindConfig maxMindConfig;
    @Mock
    private GeoIpServiceConfig geoIpServiceConfig;

    @BeforeEach
    void setUp() {
        when(geoIpServiceConfig.getMaxMindConfig()).thenReturn(maxMindConfig);
    }

    GeoIPProcessorService createObjectUnderTest() {
        return new GeoIPProcessorService(geoIpServiceConfig);
    }

    @Test
    void test_getGeoIPDatabaseReader_should_not_trigger_update_when_refresh_interval_is_high() {
        try (final MockedConstruction<GeoIPDatabaseManager> geoIPDatabaseManagerMockedConstruction = mockConstruction(GeoIPDatabaseManager.class,
                (mock,context) -> when(mock.getGeoIPDatabaseReader()).thenReturn(geoIPDatabaseReader))) {
            when(maxMindConfig.getDatabaseRefreshInterval()).thenReturn(Duration.ofHours(1));

            final GeoIPProcessorService objectUnderTest = createObjectUnderTest();
            assertThat(geoIPDatabaseManagerMockedConstruction.constructed().size(), equalTo(1));
            final GeoIPDatabaseManager geoIPDatabaseManager = geoIPDatabaseManagerMockedConstruction.constructed().get(0);

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoIPDatabaseReader));
            verify(geoIPDatabaseManager).getGeoIPDatabaseReader();
            verifyNoMoreInteractions(geoIPDatabaseManager);
        }
    }

    @Test
    void test_getGeoIPDatabaseReader_should_trigger_update_when_refresh_interval_is_met() throws InterruptedException {
        try (final MockedConstruction<GeoIPDatabaseManager> geoIPDatabaseManagerMockedConstruction = mockConstruction(GeoIPDatabaseManager.class,
                (mock, context) -> when(mock.getGeoIPDatabaseReader()).thenReturn(geoIPDatabaseReader))) {
            when(maxMindConfig.getDatabaseRefreshInterval()).thenReturn(Duration.ofNanos(1));

            final GeoIPProcessorService objectUnderTest = createObjectUnderTest();

            assertThat(geoIPDatabaseManagerMockedConstruction.constructed().size(), equalTo(1));
            final GeoIPDatabaseManager geoIPDatabaseManager = geoIPDatabaseManagerMockedConstruction.constructed().get(0);

            // Wait for next update
            Thread.sleep(1000);

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoIPDatabaseReader));

            // Wait for update to be called by ExecutorService
            Thread.sleep(1000);
            verify(geoIPDatabaseManager).updateDatabaseReader();
            verify(geoIPDatabaseManager).getGeoIPDatabaseReader();
            verifyNoMoreInteractions(geoIPDatabaseManager);
        }
    }
}
