/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.iceberg;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.time.Duration;
import java.util.List;

public class IcebergSourceConfig {

    static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(30);

    @JsonProperty("tables")
    @NotEmpty
    @Valid
    private List<TableConfig> tables;

    @JsonProperty("polling_interval")
    private Duration pollingInterval = DEFAULT_POLLING_INTERVAL;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = true;

    public List<TableConfig> getTables() {
        return tables;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public boolean isAcknowledgmentsEnabled() {
        return acknowledgments;
    }
}
