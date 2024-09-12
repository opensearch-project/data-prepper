/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class StreamConfig {

    private static final int DEFAULT_NUM_WORKERS = 1;

    // TODO: Restricted max to 1 here until we have a proper method to process the stream events in parallel.
    @JsonProperty("workers")
    @Min(1)
    @Max(1)
    private int numWorkers = DEFAULT_NUM_WORKERS;

    public int getNumWorkers() {
        return numWorkers;
    }
}
