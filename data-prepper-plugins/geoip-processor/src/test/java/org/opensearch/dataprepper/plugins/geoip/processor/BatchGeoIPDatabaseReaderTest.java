/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;
import org.opensearch.dataprepper.plugins.geoip.extension.api.GeoIPDatabaseReader;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchGeoIPDatabaseReaderTest {
    @Mock
    private GeoIPDatabaseReader geoIPDatabaseReader;

    private BatchGeoIPDatabaseReader objectUnderTestFromDecorate() {
        return BatchGeoIPDatabaseReader.decorate(geoIPDatabaseReader);
    }

    @Test
    void decorate_returns_null_if_given_null_GeoIPDatabaseReader() {
        geoIPDatabaseReader = null;
        assertThat(objectUnderTestFromDecorate(), nullValue());
    }

    @Test
    void getGeoData_returns_from_inner_geoIPDatabaseReader() {
        final InetAddress inetAddress = mock(InetAddress.class);
        final List<GeoIPField> fields = List.of(mock(GeoIPField.class));
        final Set<GeoIPDatabase> geoIPDatabases = Set.of(mock(GeoIPDatabase.class));

        final Map<String, Object> geoData = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(geoIPDatabaseReader.getGeoData(inetAddress, fields, geoIPDatabases))
                .thenReturn(geoData);

        assertThat(objectUnderTestFromDecorate().getGeoData(inetAddress, fields, geoIPDatabases),
                equalTo(geoData));
    }

    @Test
    void close_calls_inner_geoIPDataReader_close() throws Exception {
        objectUnderTestFromDecorate().close();

        verify(geoIPDatabaseReader).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isExpired_returns_inner_isExpired(final boolean isExpired) {
        when(geoIPDatabaseReader.isExpired()).thenReturn(isExpired);

        assertThat(objectUnderTestFromDecorate().isExpired(), equalTo(isExpired));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isExpired_only_calls_inner_isExpired_once(final boolean isExpired) {
        when(geoIPDatabaseReader.isExpired()).thenReturn(isExpired);

        final BatchGeoIPDatabaseReader objectUnderTest = objectUnderTestFromDecorate();

        for (int i = 0; i < 100; i++) {
            objectUnderTest.isExpired();
        }

        verify(geoIPDatabaseReader, times(1)).isExpired();
    }

    @Test
    void retain_throws_exception_because_it_should_not_be_called_by_processor() {
        final BatchGeoIPDatabaseReader objectUnderTest = objectUnderTestFromDecorate();
        assertThrows(UnsupportedOperationException.class, () -> objectUnderTest.retain());
    }
}