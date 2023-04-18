/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAlias;

/**
 * Configuration for an in_memory plugin.
 */
class InMemoryConfig {
    @JsonProperty("testing_key")
    private String testingKey;

    @JsonProperty("acknowledgements")
    @JsonAlias("acknowledgments")
    private Boolean acknowledgements = false;

    public String getTestingKey() {
        return testingKey;
    }

    public void setTestingKey(final String testingKey) {
        this.testingKey = testingKey;
    }

    public Boolean getAcknowledgements() {
        return acknowledgements;
    }

}
