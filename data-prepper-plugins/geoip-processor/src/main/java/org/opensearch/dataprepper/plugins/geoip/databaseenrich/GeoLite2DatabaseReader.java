/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.databaseenrich;

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
import org.opensearch.dataprepper.plugins.geoip.GeoIPDatabase;
import org.opensearch.dataprepper.plugins.geoip.GeoIPField;
import org.opensearch.dataprepper.plugins.geoip.exception.DatabaseReaderInitializationException;
import org.opensearch.dataprepper.plugins.geoip.exception.EngineFailureException;
import org.opensearch.dataprepper.plugins.geoip.exception.EnrichFailedException;
import org.opensearch.dataprepper.plugins.geoip.exception.NoValidDatabaseFoundException;
import org.opensearch.dataprepper.plugins.geoip.extension.databasedownload.DatabaseReaderBuilder;
import org.opensearch.dataprepper.plugins.geoip.extension.databasedownload.GeoIPFileManager;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.GEOLITE2_ASN;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.GEOLITE2_CITY;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.GEOLITE2_COUNTRY;

public class GeoLite2DatabaseReader implements GeoIPDatabaseReader, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(GeoLite2DatabaseReader.class);
    static final String MAXMIND_GEOLITE2_DATABASE_TYPE = "geolite2";
    private final DatabaseReaderBuilder databaseReaderBuilder;
    private final String databasePath;
    private final int cacheSize;
    private final AtomicBoolean isCountryDatabaseExpired;
    private final AtomicBoolean isCityDatabaseExpired;
    private final AtomicBoolean isAsnDatabaseExpired;
    private final GeoIPFileManager geoIPFileManager;
    private DatabaseReader cityDatabaseReader;
    private DatabaseReader countryDatabaseReader;
    private DatabaseReader asnDatabaseReader;
    private Instant cityDatabaseBuildDate;
    private Instant countryDatabaseBuildDate;
    private Instant asnDatabaseBuildDate;

    public GeoLite2DatabaseReader(final DatabaseReaderBuilder databaseReaderBuilder,
                                  final GeoIPFileManager geoIPFileManager,
                                  final String databasePath, final int cacheSize) {
        this.databaseReaderBuilder = databaseReaderBuilder;
        this.geoIPFileManager = geoIPFileManager;
        this.databasePath = databasePath;
        this.cacheSize = cacheSize;
        this.isCountryDatabaseExpired = new AtomicBoolean(false);
        this.isCityDatabaseExpired = new AtomicBoolean(false);
        this.isAsnDatabaseExpired = new AtomicBoolean(false);
        buildDatabaseReaders();
    }

    private void buildDatabaseReaders() {
        try {
            final Optional<String> cityDatabaseName = getDatabaseName(GEOLITE2_CITY, databasePath, MAXMIND_GEOLITE2_DATABASE_TYPE);
            final Optional<String> countryDatabaseName = getDatabaseName(GEOLITE2_COUNTRY, databasePath, MAXMIND_GEOLITE2_DATABASE_TYPE);
            final Optional<String> asnDatabaseName = getDatabaseName(GEOLITE2_ASN, databasePath, MAXMIND_GEOLITE2_DATABASE_TYPE);

            if (cityDatabaseName.isPresent()) {
                cityDatabaseReader = databaseReaderBuilder.buildReader(Path.of(databasePath + File.separator + cityDatabaseName.get()), cacheSize);
                cityDatabaseBuildDate = cityDatabaseReader.getMetadata().getBuildDate().toInstant();
            }
            if (countryDatabaseName.isPresent()) {
                countryDatabaseReader = databaseReaderBuilder.buildReader(Path.of(databasePath + File.separator + countryDatabaseName.get()), cacheSize);
                countryDatabaseBuildDate = countryDatabaseReader.getMetadata().getBuildDate().toInstant();
            }
            if (asnDatabaseName.isPresent()) {
                asnDatabaseReader = databaseReaderBuilder.buildReader(Path.of(databasePath + File.separator + asnDatabaseName.get()), cacheSize);
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
    public Map<String, Object> getGeoData(final InetAddress inetAddress, final List<GeoIPField> fields, final Set<GeoIPDatabase> geoIPDatabases) {
        final Map<String, Object> geoData = new HashMap<>();

        try {
            if (countryDatabaseReader != null && !isCountryDatabaseExpired.get() && geoIPDatabases.contains(GeoIPDatabase.COUNTRY)) {
                final Optional<CountryResponse> countryResponse = countryDatabaseReader.tryCountry(inetAddress);
                countryResponse.ifPresent(response -> processCountryResponse(response, geoData, fields));
            }

            if (cityDatabaseReader != null && !isCityDatabaseExpired.get() && geoIPDatabases.contains(GeoIPDatabase.CITY)) {
                final Optional<CityResponse> cityResponse = cityDatabaseReader.tryCity(inetAddress);
                cityResponse.ifPresent(response -> processCityResponse(response, geoData, fields, geoIPDatabases));
            }

            if (asnDatabaseReader != null && !isAsnDatabaseExpired.get() && geoIPDatabases.contains(GeoIPDatabase.ASN)) {
                final Optional<AsnResponse> asnResponse = asnDatabaseReader.tryAsn(inetAddress);
                asnResponse.ifPresent(response -> processAsnResponse(response, geoData, fields));
            }

        } catch (final GeoIp2Exception e) {
            throw new EnrichFailedException("Address not found in database.");
        } catch (final IOException e) {
            throw new EngineFailureException("Failed to close database readers gracefully. It can be due to expired databases.");
        }
        return geoData;
    }

    private void processCountryResponse(final CountryResponse countryResponse, final Map<String, Object> geoData, final List<GeoIPField> fields) {
        final Continent continent = countryResponse.getContinent();
        final Country country = countryResponse.getCountry();
        final Country registeredCountry = countryResponse.getRegisteredCountry();
        final RepresentedCountry representedCountry = countryResponse.getRepresentedCountry();


        extractContinentFields(continent, geoData, fields);
        extractCountryFields(country, geoData, fields, false);
        extractRegisteredCountryFields(registeredCountry, geoData, fields);
        extractRepresentedCountryFields(representedCountry, geoData, fields);
    }

    private void processCityResponse(final CityResponse cityResponse,
                                     final Map<String, Object> geoData,
                                     final List<GeoIPField> fields,
                                     final Set<GeoIPDatabase> geoIPDatabases) {
        // Continent and Country fields are added from City database only if they are not extracted from Country database
        if (!geoIPDatabases.contains(GeoIPDatabase.COUNTRY)) {
            final Continent continent = cityResponse.getContinent();
            final Country country = cityResponse.getCountry();
            final Country registeredCountry = cityResponse.getRegisteredCountry();
            final RepresentedCountry representedCountry = cityResponse.getRepresentedCountry();

            extractContinentFields(continent, geoData, fields);
            extractCountryFields(country, geoData, fields, false);
            extractRegisteredCountryFields(registeredCountry, geoData, fields);
            extractRepresentedCountryFields(representedCountry, geoData, fields);
        }

        final City city = cityResponse.getCity();
        final Location location = cityResponse.getLocation();
        final Postal postal = cityResponse.getPostal();
        final Subdivision mostSpecificSubdivision = cityResponse.getMostSpecificSubdivision();
        final Subdivision leastSpecificSubdivision = cityResponse.getLeastSpecificSubdivision();

        extractCityFields(city, geoData, fields, false);
        extractLocationFields(location, geoData, fields);
        extractPostalFields(postal, geoData, fields, false);
        extractMostSpecifiedSubdivisionFields(mostSpecificSubdivision, geoData, fields, false);
        extractLeastSpecifiedSubdivisionFields(leastSpecificSubdivision, geoData, fields, false);
    }

    private void processAsnResponse(final AsnResponse asnResponse, final Map<String, Object> geoData, final List<GeoIPField> fields) {
        extractAsnFields(asnResponse, geoData, fields);
    }

    @Override
    public void retain() {

    }

    @Override
    public void close() {
        closeReaders();
    }

    @Override
    public boolean isExpired() {
        final Instant instant = Instant.now();
        return isDatabaseExpired(instant, countryDatabaseReader, isCountryDatabaseExpired, countryDatabaseBuildDate, GEOLITE2_COUNTRY) &&
                isDatabaseExpired(instant, cityDatabaseReader, isCityDatabaseExpired, cityDatabaseBuildDate, GEOLITE2_CITY) &&
                isDatabaseExpired(instant, asnDatabaseReader, isAsnDatabaseExpired, asnDatabaseBuildDate, GEOLITE2_ASN);
    }

    private boolean isDatabaseExpired(final Instant instant,
                                      final DatabaseReader databaseReader,
                                      final AtomicBoolean isDatabaseExpired,
                                      final Instant databaseBuildDate,
                                      final String databaseName) {
        if (databaseReader == null) {
            // no need to delete - no action needed
            return true;
        }
        if (isDatabaseExpired.get()) {
            // Another thread already updated status to expired - no action needed
            return true;
        }
        if (databaseBuildDate.plus(MAX_EXPIRY_DURATION).isBefore(instant)) {
            isDatabaseExpired.set(true);
            closeReader(databaseReader, databaseName);
        }
        return isDatabaseExpired.get();
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
        geoIPFileManager.deleteDirectory(file);
    }

    private void closeReader(final DatabaseReader databaseReader, final String databaseName) {
        try {
            if (databaseReader != null) {
                databaseReader.close();
            }
        } catch (final IOException e) {
            LOG.debug("Failed to close Maxmind database readers due to: {}. Force closing readers.", e.getMessage());
        }

        // delete database file
        final Optional<String> fileName = getDatabaseName(databaseName, databasePath, MAXMIND_GEOLITE2_DATABASE_TYPE);
        fileName.ifPresent(response -> {
            File file = new File(databasePath + File.separator + response);
            geoIPFileManager.deleteFile(file);
        });
    }
}
