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