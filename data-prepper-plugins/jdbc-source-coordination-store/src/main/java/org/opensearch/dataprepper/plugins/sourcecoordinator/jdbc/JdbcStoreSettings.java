/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.Map;

public class JdbcStoreSettings {

    private static final String DEFAULT_TABLE_NAME = "source_coordination";
    private static final int DEFAULT_MAX_POOL_SIZE = 5;

    @JsonProperty("url")
    @NotEmpty(message = "url is required for JDBC store settings")
    private String url;

    @JsonProperty("username")
    @NotEmpty(message = "username is required for JDBC store settings")
    private String username;

    @JsonProperty("password")
    @NotEmpty(message = "password is required for JDBC store settings")
    private String password;

    @JsonProperty("table_name")
    @NotNull
    private String tableName = DEFAULT_TABLE_NAME;

    @JsonProperty("skip_table_creation")
    private boolean skipTableCreation = false;

    @JsonProperty("max_pool_size")
    @Min(1)
    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;

    @JsonProperty("ttl")
    private Duration ttl;

    @JsonProperty("connection_properties")
    private Map<String, String> connectionProperties;

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
