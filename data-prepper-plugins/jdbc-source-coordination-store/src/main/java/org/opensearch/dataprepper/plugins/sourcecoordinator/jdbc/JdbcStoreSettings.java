/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public class JdbcStoreSettings {

    private static final String DEFAULT_TABLE_NAME = "source_coordination";
    private static final int DEFAULT_MAX_POOL_SIZE = 5;

    private final String url;
    private final String username;
    private final String password;
    private final String tableName;
    private final boolean skipTableCreation;
    private final int maxPoolSize;
    private final Duration ttl;
    private final Map<String, String> connectionProperties;

    @JsonCreator
    public JdbcStoreSettings(
            @JsonProperty("url") final String url,
            @JsonProperty("username") final String username,
            @JsonProperty("password") final String password,
            @JsonProperty("table_name") final String tableName,
            @JsonProperty("skip_table_creation") final Boolean skipTableCreation,
            @JsonProperty("max_pool_size") final Integer maxPoolSize,
            @JsonProperty("ttl") final Duration ttl,
            @JsonProperty("connection_properties") final Map<String, String> connectionProperties) {
        Objects.requireNonNull(url, "url is required for JDBC store settings");
        Objects.requireNonNull(username, "username is required for JDBC store settings");
        Objects.requireNonNull(password, "password is required for JDBC store settings");

        this.url = url;
        this.username = username;
        this.password = password;
        this.tableName = tableName != null ? tableName : DEFAULT_TABLE_NAME;
        this.skipTableCreation = skipTableCreation != null ? skipTableCreation : false;
        this.maxPoolSize = maxPoolSize != null ? maxPoolSize : DEFAULT_MAX_POOL_SIZE;
        this.ttl = ttl;
        this.connectionProperties = connectionProperties;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean skipTableCreation() {
        return skipTableCreation;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public Duration getTtl() {
        return ttl;
    }

    public Map<String, String> getConnectionProperties() {
        return connectionProperties;
    }
}
