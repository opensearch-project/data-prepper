/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Manifest {
    @JsonProperty("url")
    private String url;
    @JsonProperty("db_name")
    private String dbName;
    @JsonProperty("sha256_hash")
    private String sha256Hash;
    @JsonProperty("valid_for_in_days")
    private int validForInDays;
    @JsonProperty("updated_at_in_epoch_milli")
    private long updatedAt;
    @JsonProperty("provider")
    private String provider;

    public String getUrl() {
        return url;
    }

    public String getDbName() {
        return dbName;
    }

    public String getSha256Hash() {
        return sha256Hash;
    }

    public int getValidForInDays() {
        return validForInDays;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public String getProvider() {
        return provider;
    }
}
