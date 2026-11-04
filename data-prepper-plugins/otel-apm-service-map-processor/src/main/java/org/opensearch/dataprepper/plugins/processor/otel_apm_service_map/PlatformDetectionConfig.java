/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * Platform detection: records, on each service node's {@code keyAttributes}, the platform the service
 * runs on and the low-cardinality keys that scope its infrastructure (cluster, namespace, workload,
 * region, account, {@code host.id} for hosts). The keys are descriptive: they are not part of the node
 * identity, so enabling detection changes no node hash, environment or RED metric label.
 */
public class PlatformDetectionConfig {
    static final int DEFAULT_MAX_VALUES_PER_KEY = 10;
    static final int DEFAULT_MAX_HOST_IDS = 100;

    @JsonProperty("enabled")
    @JsonPropertyDescription("Record the platform and its keys on service nodes. Disabled by default.")
    private boolean enabled = false;

    @Min(1)
    @Max(1000)
    @JsonProperty("max_values_per_key")
    @JsonPropertyDescription("Distinct values kept per key on one service-map document (e.g. a service in two " +
            "clusters keeps both). Further values are dropped.")
    private int maxValuesPerKey = DEFAULT_MAX_VALUES_PER_KEY;

    @Min(1)
    @Max(10000)
    @JsonProperty("max_host_ids")
    @JsonPropertyDescription("Distinct host.id values kept on one service-map document, for host platforms.")
    private int maxHostIds = DEFAULT_MAX_HOST_IDS;

    public boolean isEnabled() {
        return enabled;
    }

    public int getMaxValuesPerKey() {
        return maxValuesPerKey;
    }

    public int getMaxHostIds() {
        return maxHostIds;
    }

    /** The bound for one key. */
    public int maxValuesFor(final String key) {
        return "host.id".equals(key) ? maxHostIds : maxValuesPerKey;
    }
}
