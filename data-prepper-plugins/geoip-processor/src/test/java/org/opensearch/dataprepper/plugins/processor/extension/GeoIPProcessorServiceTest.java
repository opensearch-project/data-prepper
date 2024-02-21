/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.GeoIPDatabaseManager;

import java.time.Duration;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoIPProcessorServiceTest {
    @Mock
    private GeoIPDatabaseReader geoIPDatabaseReaderMock;
    @Mock
    private GeoIPDatabaseReader newGeoIPDatabaseReaderMock;
    @Mock
    private MaxMindConfig maxMindConfig;
    @Mock
    private GeoIpServiceConfig geoIpServiceConfig;
    @Mock
    private GeoIPDatabaseManager geoIPDatabaseManager;
    @Mock
    private ReentrantReadWriteLock.ReadLock readLock;

    @BeforeEach
    void setUp() {
        when(geoIpServiceConfig.getMaxMindConfig()).thenReturn(maxMindConfig);
        doNothing().when(geoIPDatabaseManager).initiateDatabaseDownload();
        when(geoIPDatabaseManager.getGeoIPDatabaseReader()).thenReturn(geoIPDatabaseReaderMock, newGeoIPDatabaseReaderMock);
    }

    GeoIPProcessorService createObjectUnderTest() {
        return new GeoIPProcessorService(geoIpServiceConfig, geoIPDatabaseManager, readLock);
    }

    @Test
    void test_getGeoIPDatabaseReader_should_not_trigger_update_when_refresh_interval_is_high() {
            when(maxMindConfig.getDatabaseRefreshInterval()).thenReturn(Duration.ofHours(1));

            final GeoIPProcessorService objectUnderTest = createObjectUnderTest();

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReaderMock, equalTo(geoIPDatabaseReader));
            verify(geoIPDatabaseManager).getGeoIPDatabaseReader();
            verifyNoMoreInteractions(geoIPDatabaseManager);
    }

    @Test
    void test_getGeoIPDatabaseReader_should_trigger_update_when_refresh_interval_is_met() throws InterruptedException {
        when(maxMindConfig.getDatabaseRefreshInterval()).thenReturn(Duration.ofNanos(1));
        doNothing().when(geoIPDatabaseManager).updateDatabaseReader();

        final GeoIPProcessorService objectUnderTest = createObjectUnderTest();

        final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
        assertThat(geoIPDatabaseReaderMock, equalTo(geoIPDatabaseReader));

        // Wait for update to be called by ExecutorService
        Thread.sleep(1000);
        verify(geoIPDatabaseManager).updateDatabaseReader();
        verify(geoIPDatabaseManager).getGeoIPDatabaseReader();
        verifyNoMoreInteractions(geoIPDatabaseManager);

        final GeoIPDatabaseReader newGeoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
        assertThat(newGeoIPDatabaseReaderMock, equalTo(newGeoIPDatabaseReader));
    }

    @Test
    void test_shutdown() throws InterruptedException {
        when(maxMindConfig.getDatabaseRefreshInterval()).thenReturn(Duration.ofNanos(1));
        doNothing().when(geoIPDatabaseManager).updateDatabaseReader();

        final GeoIPProcessorService objectUnderTest = createObjectUnderTest();

        final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
        assertThat(geoIPDatabaseReaderMock, equalTo(geoIPDatabaseReader));

        // Wait for update to be called by ExecutorService
        Thread.sleep(1000);
        verify(geoIPDatabaseManager).updateDatabaseReader();
        verify(geoIPDatabaseManager).getGeoIPDatabaseReader();
        verifyNoMoreInteractions(geoIPDatabaseManager);

        final GeoIPDatabaseReader newGeoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
        assertThat(newGeoIPDatabaseReaderMock, equalTo(newGeoIPDatabaseReader));

        assertDoesNotThrow(objectUnderTest::shutdown);
    }
}
