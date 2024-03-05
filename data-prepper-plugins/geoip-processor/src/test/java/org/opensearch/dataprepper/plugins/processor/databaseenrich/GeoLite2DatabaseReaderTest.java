/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import com.maxmind.db.Metadata;
import com.maxmind.db.Network;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.RepresentedCountry;
import com.maxmind.geoip2.record.Subdivision;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.processor.GeoIPField;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.GeoIPFileManager;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DatabaseReaderBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.GeoIPDatabase.CITY;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.ASN;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.CITY_NAME;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.CONTINENT_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.CONTINENT_NAME;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.COUNTRY_ISO_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.COUNTRY_NAME;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.IP;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.IS_COUNTRY_IN_EUROPEAN_UNION;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.LATITUDE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.LEAST_SPECIFIED_SUBDIVISION_ISO_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.LEAST_SPECIFIED_SUBDIVISION_NAME;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.LOCATION;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.LOCATION_ACCURACY_RADIUS;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.LONGITUDE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.METRO_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.MOST_SPECIFIED_SUBDIVISION_ISO_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.MOST_SPECIFIED_SUBDIVISION_NAME;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.NETWORK;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.ASN_ORGANIZATION;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.POSTAL_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.REGISTERED_COUNTRY_ISO_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.REGISTERED_COUNTRY_NAME;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.REPRESENTED_COUNTRY_ISO_CODE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.REPRESENTED_COUNTRY_NAME;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.REPRESENTED_COUNTRY_TYPE;
import static org.opensearch.dataprepper.plugins.processor.GeoIPField.TIME_ZONE;
import static org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader.LAT;
import static org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader.LON;


@ExtendWith(MockitoExtension.class)
class GeoLite2DatabaseReaderTest {
    public static final String GEOLITE2_TEST_MMDB_FILES = "./build/resources/test/mmdb-files/geo-lite2";
    public static final long ASN_RESULT = 12345L;
    public static final String ASN_ORG_RESULT = "Example Org";
    private static final String NETWORK_RESULT = "1.2.3.0/24";
    private static final String COUNTRY_NAME_RESULT = "United States";
    private static final String COUNTRY_ISO_CODE_RESULT = "US";
    private static final Boolean COUNTRY_IS_IN_EUROPEAN_UNION_RESULT = false;
    private static final String CITY_NAME_RESULT = "Los Angeles";
    private static final Double LATITUDE_RESULT = 12.34;
    private static final Double LONGITUDE_RESULT = 56.78;
    private static final Integer LOCATION_ACCURACY_RADIUS_RESULT = 10;
    private static final String TIME_ZONE_RESULT = "America/Los_Angeles";
    private static final Integer METRO_CODE_RESULT = 807;
    private static final String POSTAL_CODE_RESULT = "90001";
    private static final String LEAST_SPECIFIED_SUBDIVISION_NAME_RESULT = "California";
    private static final String MOST_SPECIFIED_SUBDIVISION_NAME_RESULT = "California";
    private static final String LEAST_SPECIFIED_SUBDIVISION_ISO_CODE_RESULT = "CA";
    private static final String MOST_SPECIFIED_SUBDIVISION_ISO_CODE_RESULT = "CA";
    private static final String REGISTERED_COUNTRY_NAME_RESULT = "Argentina";
    private static final String REGISTERED_COUNTRY_ISO_CODE_RESULT = "AR";
    private static final String REPRESENTED_COUNTRY_NAME_RESULT = "Belgium";
    private static final String REPRESENTED_COUNTRY_ISO_CODE_RESULT = "BE";
    private static final String REPRESENTED_COUNTRY_TYPE_RESULT = "military";
    private static final String CONTINENT_NAME_RESULT = "North America";
    private static final String CONTINENT_CODE_RESULT = "123456";
    private static final Map<String, Object> LOCATION_RESULT = Map.of(LAT, LATITUDE_RESULT, LON, LONGITUDE_RESULT);
    private static final String IP_RESULT = "1.2.3.4";

    @Mock
    private DatabaseReaderBuilder databaseReaderBuilder;
    @Mock
    private DatabaseReader countryDatabaseReader;
    @Mock
    private DatabaseReader cityDatabaseReader;
    @Mock
    private DatabaseReader asnDatabaseReader;
    @Mock
    private Metadata metadata;
    @Mock
    private CountryResponse countryResponse;
    @Mock
    private CityResponse cityResponse;
    @Mock
    private AsnResponse asnResponse;
    @Mock
    private Continent continent;
    @Mock
    private Country country;
    @Mock
    private RepresentedCountry representedCountry;
    @Mock
    private Country registeredCountry;
    @Mock
    private City city;
    @Mock
    private Location location;
    @Mock
    private Network network;
    @Mock
    private Postal postal;
    @Mock
    private Subdivision leastSpecificSubdivision;
    @Mock
    private Subdivision mostSpecificSubdivision;
    @Mock
    private InetAddress inetAddress;
    @Mock
    private GeoIPFileManager geoIPFileManager;

    @BeforeEach
    void setUp() throws IOException {
        when(databaseReaderBuilder.buildReader(Path.of(GEOLITE2_TEST_MMDB_FILES + File.separator + "GeoLite2-Country-Test.mmdb"), 0))
                .thenReturn(countryDatabaseReader);
        when(databaseReaderBuilder.buildReader(Path.of(GEOLITE2_TEST_MMDB_FILES + File.separator + "GeoLite2-City-Test.mmdb"), 0))
                .thenReturn(cityDatabaseReader);
        when(databaseReaderBuilder.buildReader(Path.of(GEOLITE2_TEST_MMDB_FILES + File.separator + "GeoLite2-ASN-Test.mmdb"), 0))
                .thenReturn(asnDatabaseReader);

        when(countryDatabaseReader.getMetadata()).thenReturn(metadata);
        when(cityDatabaseReader.getMetadata()).thenReturn(metadata);
        when(asnDatabaseReader.getMetadata()).thenReturn(metadata);
        final Date date = new Date(9949107436565L);
        when(metadata.getBuildDate()).thenReturn(date);

        lenient().when(countryResponse.getContinent()).thenReturn(continent);
        lenient().when(continent.getName()).thenReturn(CONTINENT_NAME_RESULT);
        lenient().when(continent.getCode()).thenReturn(CONTINENT_CODE_RESULT);

        lenient().when(countryResponse.getCountry()).thenReturn(country);
        lenient().when(country.getName()).thenReturn(COUNTRY_NAME_RESULT);
        lenient().when(country.getIsoCode()).thenReturn(COUNTRY_ISO_CODE_RESULT);
        lenient().when(country.isInEuropeanUnion()).thenReturn(COUNTRY_IS_IN_EUROPEAN_UNION_RESULT);

        lenient().when(countryResponse.getRegisteredCountry()).thenReturn(registeredCountry);
        lenient().when(registeredCountry.getName()).thenReturn(REGISTERED_COUNTRY_NAME_RESULT);
        lenient().when(registeredCountry.getIsoCode()).thenReturn(REGISTERED_COUNTRY_ISO_CODE_RESULT);

        lenient().when(countryResponse.getRepresentedCountry()).thenReturn(representedCountry);
        lenient().when(representedCountry.getName()).thenReturn(REPRESENTED_COUNTRY_NAME_RESULT);
        lenient().when(representedCountry.getIsoCode()).thenReturn(REPRESENTED_COUNTRY_ISO_CODE_RESULT);
        lenient().when(representedCountry.getType()).thenReturn(REPRESENTED_COUNTRY_TYPE_RESULT);

        lenient().when(cityResponse.getCity()).thenReturn(city);
        lenient().when(city.getName()).thenReturn(CITY_NAME_RESULT);

        lenient().when(cityResponse.getContinent()).thenReturn(continent);
        lenient().when(cityResponse.getCountry()).thenReturn(country);
        lenient().when(cityResponse.getRegisteredCountry()).thenReturn(registeredCountry);
        lenient().when(cityResponse.getRepresentedCountry()).thenReturn(representedCountry);

        lenient().when(cityResponse.getLocation()).thenReturn(location);
        lenient().when(location.getLatitude()).thenReturn(LATITUDE_RESULT);
        lenient().when(location.getLongitude()).thenReturn(LONGITUDE_RESULT);
        lenient().when(location.getMetroCode()).thenReturn(METRO_CODE_RESULT);
        lenient().when(location.getTimeZone()).thenReturn(TIME_ZONE_RESULT);
        lenient().when(location.getAccuracyRadius()).thenReturn(LOCATION_ACCURACY_RADIUS_RESULT);

        lenient().when(cityResponse.getPostal()).thenReturn(postal);
        lenient().when(postal.getCode()).thenReturn(POSTAL_CODE_RESULT);

        lenient().when(cityResponse.getLeastSpecificSubdivision()).thenReturn(leastSpecificSubdivision);
        lenient().when(leastSpecificSubdivision.getName()).thenReturn(LEAST_SPECIFIED_SUBDIVISION_NAME_RESULT);
        lenient().when(leastSpecificSubdivision.getIsoCode()).thenReturn(LEAST_SPECIFIED_SUBDIVISION_ISO_CODE_RESULT);

        lenient().when(cityResponse.getMostSpecificSubdivision()).thenReturn(mostSpecificSubdivision);
        lenient().when(mostSpecificSubdivision.getName()).thenReturn(MOST_SPECIFIED_SUBDIVISION_NAME_RESULT);
        lenient().when(mostSpecificSubdivision.getIsoCode()).thenReturn(MOST_SPECIFIED_SUBDIVISION_ISO_CODE_RESULT);

        lenient().when(asnResponse.getAutonomousSystemNumber()).thenReturn(ASN_RESULT);
        lenient().when(asnResponse.getAutonomousSystemOrganization()).thenReturn(ASN_ORG_RESULT);
        lenient().when(asnResponse.getNetwork()).thenReturn(network);
        lenient().when(asnResponse.getIpAddress()).thenReturn(IP_RESULT);
        lenient().when(network.toString()).thenReturn(NETWORK_RESULT);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(countryDatabaseReader);
        verifyNoMoreInteractions(cityDatabaseReader);
        verifyNoMoreInteractions(asnDatabaseReader);
    }

    GeoLite2DatabaseReader createObjectUnderTest() {
        return new GeoLite2DatabaseReader(databaseReaderBuilder, geoIPFileManager, GEOLITE2_TEST_MMDB_FILES, 0);
    }

    @Test
    void test_getGeoData_for_all_fields_in_country_database() throws IOException, GeoIp2Exception {
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        final List<GeoIPField> fields = List.of(CONTINENT_NAME, CONTINENT_CODE, COUNTRY_NAME, COUNTRY_ISO_CODE, IS_COUNTRY_IN_EUROPEAN_UNION,
                REPRESENTED_COUNTRY_NAME, REPRESENTED_COUNTRY_ISO_CODE, REPRESENTED_COUNTRY_TYPE, REGISTERED_COUNTRY_NAME,
                REGISTERED_COUNTRY_ISO_CODE);
        final Set<GeoIPDatabase> databases = Set.of(GeoIPDatabase.COUNTRY);

        when(countryDatabaseReader.tryCountry(inetAddress)).thenReturn(Optional.of(countryResponse));

        final Map<String, Object> geoData = objectUnderTest.getGeoData(inetAddress, fields, databases);
        assertThat(geoData.size(), equalTo(fields.size()));
        assertThat(geoData.get(CONTINENT_NAME.getFieldName()), equalTo(CONTINENT_NAME_RESULT));
        assertThat(geoData.get(CONTINENT_CODE.getFieldName()), equalTo(CONTINENT_CODE_RESULT));
        assertThat(geoData.get(COUNTRY_NAME.getFieldName()), equalTo(COUNTRY_NAME_RESULT));
        assertThat(geoData.get(COUNTRY_ISO_CODE.getFieldName()), equalTo(COUNTRY_ISO_CODE_RESULT));
        assertThat(geoData.get(IS_COUNTRY_IN_EUROPEAN_UNION.getFieldName()), equalTo(COUNTRY_IS_IN_EUROPEAN_UNION_RESULT));
        assertThat(geoData.get(REGISTERED_COUNTRY_NAME.getFieldName()), equalTo(REGISTERED_COUNTRY_NAME_RESULT));
        assertThat(geoData.get(REGISTERED_COUNTRY_ISO_CODE.getFieldName()), equalTo(REGISTERED_COUNTRY_ISO_CODE_RESULT));
        assertThat(geoData.get(REPRESENTED_COUNTRY_NAME.getFieldName()), equalTo(REPRESENTED_COUNTRY_NAME_RESULT));
        assertThat(geoData.get(REPRESENTED_COUNTRY_ISO_CODE.getFieldName()), equalTo(REPRESENTED_COUNTRY_ISO_CODE_RESULT));
        assertThat(geoData.get(REPRESENTED_COUNTRY_TYPE.getFieldName()), equalTo(REPRESENTED_COUNTRY_TYPE_RESULT));
    }

    @Test
    void test_getGeoData_for_all_fields_in_city_database() throws IOException, GeoIp2Exception {
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        final List<GeoIPField> fields = List.of(CONTINENT_NAME, CONTINENT_CODE, COUNTRY_NAME, COUNTRY_ISO_CODE, IS_COUNTRY_IN_EUROPEAN_UNION,
                REPRESENTED_COUNTRY_NAME, REPRESENTED_COUNTRY_ISO_CODE, REPRESENTED_COUNTRY_TYPE, REGISTERED_COUNTRY_NAME,
                REGISTERED_COUNTRY_ISO_CODE, CITY_NAME, LOCATION, LOCATION_ACCURACY_RADIUS, LATITUDE, LONGITUDE, METRO_CODE, TIME_ZONE, POSTAL_CODE,
                MOST_SPECIFIED_SUBDIVISION_NAME, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, LEAST_SPECIFIED_SUBDIVISION_NAME,
                LEAST_SPECIFIED_SUBDIVISION_ISO_CODE);
        final Set<GeoIPDatabase> databases = Set.of(CITY);

        when(cityDatabaseReader.tryCity(inetAddress)).thenReturn(Optional.of(cityResponse));

        final Map<String, Object> geoData = objectUnderTest.getGeoData(inetAddress, fields, databases);

        assertThat(geoData.size(), equalTo(fields.size()));
        assertThat(geoData.get(CONTINENT_NAME.getFieldName()), equalTo(CONTINENT_NAME_RESULT));
        assertThat(geoData.get(CONTINENT_CODE.getFieldName()), equalTo(CONTINENT_CODE_RESULT));
        assertThat(geoData.get(COUNTRY_NAME.getFieldName()), equalTo(COUNTRY_NAME_RESULT));
        assertThat(geoData.get(COUNTRY_ISO_CODE.getFieldName()), equalTo(COUNTRY_ISO_CODE_RESULT));
        assertThat(geoData.get(IS_COUNTRY_IN_EUROPEAN_UNION.getFieldName()), equalTo(COUNTRY_IS_IN_EUROPEAN_UNION_RESULT));
        assertThat(geoData.get(REGISTERED_COUNTRY_NAME.getFieldName()), equalTo(REGISTERED_COUNTRY_NAME_RESULT));
        assertThat(geoData.get(REGISTERED_COUNTRY_ISO_CODE.getFieldName()), equalTo(REGISTERED_COUNTRY_ISO_CODE_RESULT));
        assertThat(geoData.get(REPRESENTED_COUNTRY_NAME.getFieldName()), equalTo(REPRESENTED_COUNTRY_NAME_RESULT));
        assertThat(geoData.get(REPRESENTED_COUNTRY_ISO_CODE.getFieldName()), equalTo(REPRESENTED_COUNTRY_ISO_CODE_RESULT));
        assertThat(geoData.get(REPRESENTED_COUNTRY_TYPE.getFieldName()), equalTo(REPRESENTED_COUNTRY_TYPE_RESULT));

        assertThat(geoData.get(CITY_NAME.getFieldName()), equalTo(CITY_NAME_RESULT));
        assertThat(geoData.get(LOCATION.getFieldName()), equalTo(LOCATION_RESULT));
        assertThat(geoData.get(LATITUDE.getFieldName()), equalTo(LATITUDE_RESULT));
        assertThat(geoData.get(LONGITUDE.getFieldName()), equalTo(LONGITUDE_RESULT));
        assertThat(geoData.get(LOCATION_ACCURACY_RADIUS.getFieldName()), equalTo(LOCATION_ACCURACY_RADIUS_RESULT));
        assertThat(geoData.get(METRO_CODE.getFieldName()), equalTo(METRO_CODE_RESULT));
        assertThat(geoData.get(TIME_ZONE.getFieldName()), equalTo(TIME_ZONE_RESULT));
        assertThat(geoData.get(POSTAL_CODE.getFieldName()), equalTo(POSTAL_CODE_RESULT));
        assertThat(geoData.get(MOST_SPECIFIED_SUBDIVISION_NAME.getFieldName()), equalTo(MOST_SPECIFIED_SUBDIVISION_NAME_RESULT));
        assertThat(geoData.get(MOST_SPECIFIED_SUBDIVISION_ISO_CODE.getFieldName()), equalTo(MOST_SPECIFIED_SUBDIVISION_ISO_CODE_RESULT));
        assertThat(geoData.get(LEAST_SPECIFIED_SUBDIVISION_NAME.getFieldName()), equalTo(LEAST_SPECIFIED_SUBDIVISION_NAME_RESULT));
        assertThat(geoData.get(LEAST_SPECIFIED_SUBDIVISION_ISO_CODE.getFieldName()), equalTo(LEAST_SPECIFIED_SUBDIVISION_ISO_CODE_RESULT));
    }

    @Test
    void test_getGeoData_for_all_fields_in_asn_database() throws IOException, GeoIp2Exception {
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        final List<GeoIPField> fields = List.of(ASN, ASN_ORGANIZATION, NETWORK, IP);
        final Set<GeoIPDatabase> databases = Set.of(GeoIPDatabase.ASN);

        when(asnDatabaseReader.tryAsn(inetAddress)).thenReturn(Optional.of(asnResponse));

        final Map<String, Object> geoData = objectUnderTest.getGeoData(inetAddress, fields, databases);

        assertThat(geoData.size(), equalTo(fields.size()));
        assertThat(geoData.get(ASN.getFieldName()), equalTo(ASN_RESULT));
        assertThat(geoData.get(ASN_ORGANIZATION.getFieldName()), equalTo(ASN_ORG_RESULT));
        assertThat(geoData.get(NETWORK.getFieldName()), equalTo(NETWORK_RESULT));
        assertThat(geoData.get(IP.getFieldName()), equalTo(IP_RESULT));
    }

    @Test
    void test_getGeoData_for_country_database_should_not_add_any_fields_if_country_is_not_required() {
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        final List<GeoIPField> fields = List.of(CONTINENT_NAME, CONTINENT_CODE, COUNTRY_NAME);
        final Set<GeoIPDatabase> databases = Set.of();

        final Map<String, Object> geoData = objectUnderTest.getGeoData(inetAddress, fields, databases);
        assertThat(geoData.size(), equalTo(0));
    }

    @Test
    void test_getGeoData_for_city_database_should_not_add_any_fields_if_city_is_not_required() {
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        final List<GeoIPField> fields = List.of(CONTINENT_NAME, CONTINENT_CODE, COUNTRY_NAME);
        final Set<GeoIPDatabase> databases = Set.of();

        final Map<String, Object> geoData = objectUnderTest.getGeoData(inetAddress, fields, databases);

        assertThat(geoData.size(), equalTo(0));
    }

    @Test
    void test_getGeoData_for_asn_database_should_not_add_any_fields_if_asn_is_not_required() {
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        final List<GeoIPField> fields = List.of(ASN, ASN_ORGANIZATION, NETWORK);
        final Set<GeoIPDatabase> databases = Set.of();

        final Map<String, Object> geoData = objectUnderTest.getGeoData(inetAddress, fields, databases);

        assertThat(geoData.size(), equalTo(0));
    }

    @Test
    void test_database_expired_should_return_false_when_expiry_date_is_in_future() {
        final Date date = new Date(9949107436565L);
        when(metadata.getBuildDate()).thenReturn(date);
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isExpired(), equalTo(false));
    }

    @Test
    void test_database_expired_should_return_true_when_expiry_date_is_in_past() throws IOException {
        final Date date = new Date(91911199999L);
        when(metadata.getBuildDate()).thenReturn(date);
        doNothing().when(geoIPFileManager).deleteFile(any());
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isExpired(), equalTo(true));
        verify(countryDatabaseReader).close();
        verify(cityDatabaseReader).close();
        verify(asnDatabaseReader).close();
    }

    @Test
    void test_database_close_should_close_the_reader() throws IOException {
        doNothing().when(geoIPFileManager).deleteDirectory(any());
        final GeoLite2DatabaseReader objectUnderTest = createObjectUnderTest();

        objectUnderTest.close();

        assertThat(objectUnderTest.isExpired(), equalTo(false));
        verify(countryDatabaseReader).close();
        verify(cityDatabaseReader).close();
        verify(asnDatabaseReader).close();
    }

}
