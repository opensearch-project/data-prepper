/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.event_json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventJsonInputCodecConfig {
    @JsonProperty("override_time_received")
    private Boolean overrideTimeReceived = false;

    public Boolean getOverrideTimeReceived() {
        return overrideTimeReceived;
    }
}


