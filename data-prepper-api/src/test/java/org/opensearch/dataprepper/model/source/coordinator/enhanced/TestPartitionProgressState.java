/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TestPartitionProgressState {

    @JsonProperty("testValue")
    private String testValue;

    public TestPartitionProgressState(@JsonProperty("testValue") final String testValue) {
        this.testValue = testValue;
    }

    public String getTestValue() {
        return testValue;
    }

    public void setTestValue(final String testValue) { this.testValue = testValue; }
}