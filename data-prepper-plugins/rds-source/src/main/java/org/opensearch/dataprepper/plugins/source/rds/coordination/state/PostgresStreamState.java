/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Set;

public class PostgresStreamState {

    @JsonProperty("currentLsn")
    private String currentLsn;

    @JsonProperty("publicationName")
    private String publicationName;

    @JsonProperty("replicationSlotName")
    private String replicationSlotName;

    @JsonProperty("enumColumnsByTable")
    private Map<String, Set<String>> enumColumnsByTable;

    public String getCurrentLsn() {
        return currentLsn;
    }

    public void setCurrentLsn(String currentLsn) {
        this.currentLsn = currentLsn;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public void setPublicationName(String publicationName) {
        this.publicationName = publicationName;
    }

    public String getReplicationSlotName() {
        return replicationSlotName;
    }

    public void setReplicationSlotName(String replicationSlotName) {
        this.replicationSlotName = replicationSlotName;
    }

    public Map<String, Set<String>> getEnumColumnsByTable() {
        return enumColumnsByTable;
    }

    public void setEnumColumnsByTable(Map<String, Set<String>> enumColumnsByTable) {
        this.enumColumnsByTable = enumColumnsByTable;
    }
}
