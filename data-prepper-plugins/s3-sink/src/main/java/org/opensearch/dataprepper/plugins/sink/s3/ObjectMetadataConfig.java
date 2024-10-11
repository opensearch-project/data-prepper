/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
public class ObjectMetadataConfig {
    @JsonProperty("number_of_events_key")
    private String numberOfEventsKey;

    public String getNumberOfEventsKey() {
        return numberOfEventsKey;
    }

}
