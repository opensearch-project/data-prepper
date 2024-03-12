/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamConfig {
    
    @JsonProperty(value = "start_position")
    private StreamStartPosition startPosition = StreamStartPosition.LATEST;

    @JsonProperty("use_old_image_for_deletes")
    private boolean useOldImageForDeletes = false;

    public StreamStartPosition getStartPosition() {
        return startPosition;
    }

    public boolean shouldUseOldImageForDeletes() {
        return useOldImageForDeletes;
    }

}
