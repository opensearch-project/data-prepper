/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

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
import com.maxmind.geoip2.record.Subdivision;
import org.opensearch.dataprepper.plugins.processor.exception.DatabaseReaderInitializationException;
import org.opensearch.dataprepper.plugins.processor.exception.EnrichFailedException;
import org.opensearch.dataprepper.plugins.processor.exception.NoValidDatabaseFoundException;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DBSource;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.DatabaseReaderCreate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class GeoLite2DatabaseReader implements GeoIPDatabaseReader, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(GeoLite2DatabaseReader.class);
    private static final String MAXMIND_GEOLITE2_DATABASE_TYPE = "geolite2";
    private static final String CITY_DATABASE = "city";
    private static final String COUNTRY_DATABASE = "country";
    private static final String ASN_DATABASE = "asn";
    private final String databasePath;
    private final int cacheSize;
    private final AtomicInteger closeCount;
    private DatabaseReader cityDatabaseReader;
    private DatabaseReader countryDatabaseReader;
    private DatabaseReader asnDatabaseReader;
    private Instant cityDatabaseBuildDate;
    private Instant countryDatabaseBuildDate;
    private Instant asnDatabaseBuildDate;

    public GeoLite2DatabaseReader(final String databasePath, final int cacheSize) {
        this.databasePath = databasePath;
        this.cacheSize = cacheSize;
        this.closeCount = new AtomicInteger(1);
        buildDatabaseReaders();
    }

    private void buildDatabaseReaders() {
        try {
            final Optional<String> cityDatabaseName = getDatabaseName(CITY_DATABASE, databasePath, MAXMIND_GEOLITE2_DATABASE_TYPE);
            final Optional<String> countryDatabaseName = getDatabaseName(COUNTRY_DATABASE, databasePath, MAXMIND_GEOLITE2_DATABASE_TYPE);
            final Optional<String> asnDatabaseName = getDatabaseName(ASN_DATABASE, databasePath, MAXMIND_GEOLITE2_DATABASE_TYPE);

            if (cityDatabaseName.isPresent()) {
                cityDatabaseReader = DatabaseReaderCreate.buildReader(Path.of(databasePath + File.separator + cityDatabaseName.get()), cacheSize);
                cityDatabaseBuildDate = cityDatabaseReader.getMetadata().getBuildDate().toInstant();
            }
            if (countryDatabaseName.isPresent()) {
                countryDatabaseReader = DatabaseReaderCreate.buildReader(Path.of(databasePath + File.separator + countryDatabaseName.get()), cacheSize);
                countryDatabaseBuildDate = countryDatabaseReader.getMetadata().getBuildDate().toInstant();
            }
            if (asnDatabaseName.isPresent()) {
                asnDatabaseReader = DatabaseReaderCreate.buildReader(Path.of(databasePath + File.separator + asnDatabaseName.get()), cacheSize);
                asnDatabaseBuildDate = asnDatabaseReader.getMetadata().getBuildDate().toInstant();
            }

        } catch (final IOException ex) {
            throw new DatabaseReaderInitializationException("Exception while creating GeoLite2 DatabaseReaders due to: " + ex.getMessage());
        }

        if (cityDatabaseReader == null && countryDatabaseReader == null && asnDatabaseReader == null) {
            throw new NoValidDatabaseFoundException("Unable to initialize any GeoLite2 database, make sure they are valid.");
        }
    }

    @Override
    public Map<String, Object> getGeoData(final InetAddress inetAddress, final List<String> fields, final Set<GeoIPDatabase> geoIPDatabases) {
        final Map<String, Object> geoData = new HashMap<>();

        try {
            if (geoIPDatabases.contains(GeoIPDatabase.COUNTRY)) {
                final Optional<CountryResponse> countryResponse = countryDatabaseReader.tryCountry(inetAddress);
                countryResponse.ifPresent(response -> processCountryResponse(response, geoData, fields));
            }

            if (geoIPDatabases.contains(GeoIPDatabase.CITY)) {
                final Optional<CityResponse> cityResponse = cityDatabaseReader.tryCity(inetAddress);
                cityResponse.ifPresent(response -> processCityResponse(response, geoData, fields));
            }

            if (geoIPDatabases.contains(GeoIPDatabase.ASN)) {
                final Optional<AsnResponse> asnResponse = asnDatabaseReader.tryAsn(inetAddress);
                asnResponse.ifPresent(response -> processAsnResponse(response, geoData, fields));
            }

        } catch (final GeoIp2Exception e) {
            throw new EnrichFailedException("Address not found in database.");
        } catch (final IOException e) {
            throw new EnrichFailedException("IO Exception: " + e.getMessage());
        }
        return geoData;
    }

    private void processCountryResponse(final CountryResponse countryResponse, final Map<String, Object> geoData, final List<String> fields) {
        final Continent continent = countryResponse.getContinent();
        final Country country = countryResponse.getCountry();

        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case CONTINENT_CODE:
                        enrichData(geoData, CONTINENT_CODE, continent.getCode());
                        break;
                    case CONTINENT_NAME:
                        enrichData(geoData, CONTINENT_NAME, continent.getName());
                        break;
                    case COUNTRY_NAME:
                        enrichData(geoData, COUNTRY_NAME, country.getName());
                        break;
                    case IS_COUNTRY_IN_EUROPEAN_UNION:
                        enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
                        break;
                    case COUNTRY_ISO_CODE:
                        enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, CONTINENT_CODE, continent.getCode());
            enrichData(geoData, CONTINENT_NAME, continent.getName());
            enrichData(geoData, COUNTRY_NAME, country.getName());
            enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
            enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
        }
    }

    private void processCityResponse(final CityResponse cityResponse, final Map<String, Object> geoData, final List<String> fields) {
        // Continent and Country fields are added from City database only if they are not extracted from Country database
        final Continent continent = cityResponse.getContinent();
        final Country country = cityResponse.getCountry();

        final City city = cityResponse.getCity();
        final Location location = cityResponse.getLocation();
        final Postal postal = cityResponse.getPostal();
        final Subdivision mostSpecificSubdivision = cityResponse.getMostSpecificSubdivision();

        final Map<String, Object> locationObject = new HashMap<>();
        locationObject.put(LATITUDE, location.getLatitude());
        locationObject.put(LONGITUDE, location.getLongitude());

        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case CONTINENT_CODE:
                        enrichData(geoData, CONTINENT_CODE, continent.getCode());
                        break;
                    case CONTINENT_NAME:
                        enrichData(geoData, CONTINENT_NAME, continent.getName());
                        break;
                    case COUNTRY_NAME:
                        enrichData(geoData, COUNTRY_NAME, country.getName());
                        break;
                    case IS_COUNTRY_IN_EUROPEAN_UNION:
                        enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
                        break;
                    case COUNTRY_ISO_CODE:
                        enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
                        break;
                    case CITY_NAME:
                        enrichData(geoData, CITY_NAME, city.getName());
                        break;
                    case LOCATION:
                        enrichData(geoData, LOCATION, locationObject);
                        break;
                    case LATITUDE:
                        enrichData(geoData, LATITUDE, location.getLatitude());
                        break;
                    case LONGITUDE:
                        enrichData(geoData, LONGITUDE, location.getLongitude());
                        break;
                    case METRO_CODE:
                        enrichData(geoData, METRO_CODE, location.getMetroCode());
                        break;
                    case TIME_ZONE:
                        enrichData(geoData, TIME_ZONE, location.getTimeZone());
                        break;
                    case POSTAL_CODE:
                        enrichData(geoData, POSTAL_CODE, postal.getCode());
                        break;
                    case MOST_SPECIFIED_SUBDIVISION_NAME:
                        enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_NAME, mostSpecificSubdivision.getName());
                        break;
                    case MOST_SPECIFIED_SUBDIVISION_ISO_CODE:
                        enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, mostSpecificSubdivision.getIsoCode());
                        break;
                }
            }
        } else{
            // add all fields - latitude & longitude will be part of location key
            enrichData(geoData, CONTINENT_CODE, continent.getCode());
            enrichData(geoData, CONTINENT_NAME, continent.getName());
            enrichData(geoData, COUNTRY_NAME, country.getName());
            enrichData(geoData, IS_COUNTRY_IN_EUROPEAN_UNION, country.isInEuropeanUnion());
            enrichData(geoData, COUNTRY_ISO_CODE, country.getIsoCode());
            enrichData(geoData, CITY_NAME, city.getName());
            enrichData(geoData, LOCATION, locationObject);
            enrichData(geoData, METRO_CODE, location.getMetroCode());
            enrichData(geoData, TIME_ZONE, location.getTimeZone());
            enrichData(geoData, POSTAL_CODE, postal.getCode());
            enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_NAME, mostSpecificSubdivision.getName());
            enrichData(geoData, MOST_SPECIFIED_SUBDIVISION_ISO_CODE, mostSpecificSubdivision.getIsoCode());
        }
    }

    private void processAsnResponse(final AsnResponse asnResponse, final Map<String, Object> geoData, final List<String> fields) {
        if (!fields.isEmpty()) {
            for (final String field : fields) {
                switch (field) {
                    case ASN:
                        enrichData(geoData, ASN, asnResponse.getAutonomousSystemNumber());
                        break;
                    case ORGANIZATION:
                        enrichData(geoData, ORGANIZATION, asnResponse.getAutonomousSystemOrganization());
                        break;
                    case NETWORK:
                        enrichData(geoData, NETWORK, asnResponse.getNetwork().toString());
                        break;
                }
            }
        } else {
            // add all fields
            enrichData(geoData, ASN, asnResponse.getAutonomousSystemNumber());
            enrichData(geoData, ORGANIZATION, asnResponse.getAutonomousSystemOrganization());
            enrichData(geoData, NETWORK, asnResponse.getNetwork().toString());
        }
    }

    @Override
    public boolean areDatabasesExpired() {
        final Instant instant = Instant.now();
        return isAsnDatabaseExpired(instant) && isCountryDatabaseExpired(instant) && isCityDatabaseExpired(instant);
    }

    private boolean isCountryDatabaseExpired (final Instant instant) {
        if (countryDatabaseReader != null && countryDatabaseBuildDate.plus(MAX_EXPIRY_DURATION).isBefore(instant)) {
            return true;
        } else return countryDatabaseReader == null;
    }

    private boolean isCityDatabaseExpired (final Instant instant) {
        if (cityDatabaseReader != null && cityDatabaseBuildDate.plus(MAX_EXPIRY_DURATION).isBefore(instant)) {
            return true;
        } else return cityDatabaseReader == null;
    }

    private boolean isAsnDatabaseExpired (final Instant instant) {
        if (asnDatabaseReader != null && asnDatabaseBuildDate.plus(MAX_EXPIRY_DURATION).isBefore(instant)) {
            return true;
        } else return asnDatabaseReader == null;
    }

    @Override
    public void retain() {
        closeCount.incrementAndGet();
        LOG.info("Retain: {}", closeCount);
    }

    @Override
    public void close() {
        final int i = closeCount.decrementAndGet();
        LOG.info("Close: {}", i);
        if (i == 0) {
            LOG.info("Closing old readers");
            closeReaders();
        }
    }

    private void closeReaders() {
        try {
            if (cityDatabaseReader != null) {
                cityDatabaseReader.close();
            }
            if (countryDatabaseReader != null) {
                countryDatabaseReader.close();
            }
            if (asnDatabaseReader != null) {
                asnDatabaseReader.close();
            }
        } catch (final IOException e) {
            LOG.debug("Failed to close Maxmind database readers due to: {}. Force closing readers.", e.getMessage());
        }

        // delete database directory
        final File file = new File(databasePath);
        DBSource.deleteDirectory(file);
    }
}
