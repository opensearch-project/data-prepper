/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

public class StreamConfig {
    
    @JsonProperty(value = "start_position")
    private StreamStartPosition startPosition = StreamStartPosition.LATEST;

    @JsonProperty("view_on_remove")
    private StreamViewType viewForRemoves = StreamViewType.NEW_IMAGE;

    public StreamStartPosition getStartPosition() {
        return startPosition;
    }

    public StreamViewType getStreamViewForRemoves() {
        return viewForRemoves;
    }

}
