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

public class PostgresStreamState {

    @JsonProperty("currentLsn")
    private String currentLsn;

    @JsonProperty("replicationSlotName")
    private String replicationSlotName;

    public String getCurrentLsn() {
        return currentLsn;
    }

    public void setCurrentLsn(String currentLsn) {
        this.currentLsn = currentLsn;
    }

    public String getReplicationSlotName() {
        return replicationSlotName;
    }

    public void setReplicationSlotName(String replicationSlotName) {
        this.replicationSlotName = replicationSlotName;
    }
}
