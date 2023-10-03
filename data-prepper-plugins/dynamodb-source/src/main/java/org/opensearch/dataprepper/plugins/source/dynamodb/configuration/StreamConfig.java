/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamConfig {
    
    @JsonProperty(value = "start_position")
    private String startPosition;

    public String getStartPosition() {
        return startPosition;
    }

}
