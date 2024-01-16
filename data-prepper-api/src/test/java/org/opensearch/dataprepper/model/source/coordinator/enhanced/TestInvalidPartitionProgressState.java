/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestInvalidPartitionProgressState {

    @JsonProperty("invalid")
    private String x;

    @JsonProperty("invalid")
    private String y;
}
