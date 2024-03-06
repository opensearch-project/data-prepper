/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.databaseenrich;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.ASN;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.ASN_ORGANIZATION;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.IP;
import static org.opensearch.dataprepper.plugins.geoip.GeoIPField.NETWORK;

@ExtendWith(MockitoExtension.class)
class AutoCountingDatabaseReaderTest {
    @Mock
    private GeoLite2DatabaseReader geoLite2DatabaseReader;
    @Mock
    private InetAddress inetAddress;

    GeoIPDatabaseReader createObjectUnderTest() {
        return new AutoCountingDatabaseReader(geoLite2DatabaseReader);
    }

    @Test
    void test_database_close_should_not_close_the_reader_if_close_count_is_not_zero() throws Exception {
        final GeoIPDatabaseReader objectUnderTest = createObjectUnderTest();

        objectUnderTest.retain();
        objectUnderTest.close();

        verifyNoMoreInteractions(geoLite2DatabaseReader);
    }

    @Test
    void test_database_close_should_close_the_reader_when_close_count_is_zero() throws Exception {
        final GeoIPDatabaseReader objectUnderTest = createObjectUnderTest();

        objectUnderTest.retain();
        objectUnderTest.close();
        objectUnderTest.close();

        verify(geoLite2DatabaseReader).close();
    }

    @Test
    void test_getGeoData_should_call_delegate_reader_getGeoData() {
        final GeoIPDatabaseReader objectUnderTest = createObjectUnderTest();

        final Set<GeoIPDatabase> databases = Set.of(GeoIPDatabase.ASN);
        final List<GeoIPField> fields = List.of(ASN, ASN_ORGANIZATION, NETWORK, IP);
        objectUnderTest.getGeoData(inetAddress, fields, databases);

        verify(geoLite2DatabaseReader).getGeoData(inetAddress, fields, databases);
    }

    @Test
    void test_isExpired_should_call_delegate_reader_isExpired() {
        final GeoIPDatabaseReader objectUnderTest = createObjectUnderTest();

        objectUnderTest.isExpired();

        verify(geoLite2DatabaseReader).isExpired();
    }

}