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