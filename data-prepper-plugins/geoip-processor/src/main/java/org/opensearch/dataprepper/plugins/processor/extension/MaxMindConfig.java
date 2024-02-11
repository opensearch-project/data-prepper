/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.plugins.processor.utils.DatabaseSourceIdentification;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class MaxMindConfig {
    private static final boolean DEFAULT_INSECURE = false;
    private static final String S3_PREFIX = "s3://";

    //TODO: Add validations to database paths
    //TODO: Make default path to be a public CDN endpoint
    private static final List<String> DEFAULT_DATABASE_PATHS = new ArrayList<>();
    private static final Duration DEFAULT_DATABASE_REFRESH_INTERVAL = Duration.ofDays(7);
    private static final int DEFAULT_CACHE_SIZE = 4096;
    private static final String DEFAULT_DATABASE_DESTINATION = System.getProperty("data-prepper.dir") + File.separator + "data" + File.separator + "geoip";

    @JsonProperty("database_paths")
    private List<String> databasePaths = DEFAULT_DATABASE_PATHS;

    @JsonProperty("database_refresh_interval")
    @DurationMin(days = 1)
    @DurationMax(days = 30)
    private Duration databaseRefreshInterval = DEFAULT_DATABASE_REFRESH_INTERVAL;

    @JsonProperty("cache_size")
    @Min(1)
    //TODO:  Add a Max limit on cache size
    private int cacheSize = DEFAULT_CACHE_SIZE;

    //TODO: Add a destination path to store database files
    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig;

    @JsonProperty("insecure")
    private boolean insecure = DEFAULT_INSECURE;

    @JsonProperty("database_destination_path")
    private String databaseDestination = DEFAULT_DATABASE_DESTINATION;

    public MaxMindConfig() {
        // This default constructor is used if maxmind is not configured
    }

    @AssertTrue(message = "aws should be configured if any path in database_paths is S3 bucket path.")
    public boolean isAwsAuthenticationOptionsRequired() {
        for (final String databasePath : databasePaths) {
            if (databasePath.startsWith(S3_PREFIX)) {
                return awsAuthenticationOptionsConfig != null;
            }
        }
        return true;
    }

    @AssertTrue(message = "database_paths should be https endpoint if using URL and if insecure is set to false")
    public boolean isSecureEndpoint() throws URISyntaxException {
        if (insecure) {
            return true;
        }
        for (final String databasePath : databasePaths) {
            if (DatabaseSourceIdentification.isURL(databasePath)) {
                return new URI(databasePath).getScheme().equals("https");
            }
        }
        return true;
    }

    /**
     * Gets the MaxMind database paths
     *
     * @return The MaxMind database paths
     * @since 2.7
     */
    public List<String> getDatabasePaths() {
        return databasePaths;
    }

    /**
     * Gets the database refresh duration. This loads the database from the paths into memory again in case if there are any updates.
     *
     * @return The refresh duration
     * @since 2.7
     */
    public Duration getDatabaseRefreshInterval() {
        return databaseRefreshInterval;
    }

    /**
     * Gets the cache size used in CHM cache for MaxMind DatabaseReader.
     *
     * @return The cache size
     * @since 2.7
     */
    public int getCacheSize() {
        return cacheSize;
    }

    /**
     * Gets the AWS authentication config used for reading from S3 bucket
     *
     * @return The AWS authentication options
     * @since 2.7
     */
    public AwsAuthenticationOptionsConfig getAwsAuthenticationOptionsConfig() {
        return awsAuthenticationOptionsConfig;
    }

    /**
     * Gets the destination folder to store database files
     *
     * @return The destination folder
     * @since 2.7
     */
    //TODO: Use this instead of temp directory
    public String getDatabaseDestination() {
        return databaseDestination;
    }
}
