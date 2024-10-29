/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class TestInstantTypeProgressState {

    @JsonProperty("testValue")
    private Instant testValue;

    public TestInstantTypeProgressState(@JsonProperty("testValue") final Instant testValue) {
        this.testValue = testValue;
    }

    public Instant getTestValue() {
        return testValue;
    }

}