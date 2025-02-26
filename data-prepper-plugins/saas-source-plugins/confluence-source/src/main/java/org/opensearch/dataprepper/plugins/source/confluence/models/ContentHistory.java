/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.confluence.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Setter
@Getter
public class ContentHistory {

    // Example format "createdDate": "2025-02-17T23:34:44.633Z"
    // Jackson converts to Instant type
    @JsonProperty("createdDate")
    Instant createdDate;

    @JsonProperty("lastUpdated")
    LastUpdated lastUpdated;

    /**
     * @return milliseconds since epoch, or 0 if createdDate is null or invalid
     */
    public long getCreatedDateInMillis() {
        return (createdDate != null) ? createdDate.toEpochMilli() : 0L;
    }

    public long getLastUpdatedInMillis() {
        return (lastUpdated != null && lastUpdated.when != null) ? lastUpdated.when.toEpochMilli() : 0L;
    }

    @Setter
    @Getter
    public static class LastUpdated {
        // Example format  "when": "2025-02-17T23:34:44.633Z"
        @JsonProperty("when")
        Instant when;
    }


}
