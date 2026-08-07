/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestInvalidPartitionProgressState {

    @JsonProperty("invalid")
    private String x;

    @JsonProperty("invalid")
    private String y;
}
