/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamConfig {

    public enum StartPosition {
        LATEST,
        BEGINNING
    }

    @JsonProperty(value = "start_position")
    private StartPosition startPosition;
    
    public StartPosition getStartPosition() {
        return startPosition;
    }

}
