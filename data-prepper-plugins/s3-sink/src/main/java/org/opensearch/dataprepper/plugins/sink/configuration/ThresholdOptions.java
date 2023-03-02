/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;

/**
    An implementation class of s3 index configuration Options
 */
public class ThresholdOptions {
    static final int DEFAULT_EVENT_COUNT = 200;
    private static final long DEFAULT_BYTE_CAPACITY = 2500;
    private static final long DEFAULT_TIMEOUT = 20;

    @JsonProperty("event_count")
    @NotNull
    private int eventCount = DEFAULT_EVENT_COUNT;

    @JsonProperty("byte_capacity")
    @NotNull
    private long byteCapacity = DEFAULT_BYTE_CAPACITY;

    @JsonProperty("event_collection_duration")
    private long eventCollectionDuration = DEFAULT_TIMEOUT;

    /*
       Read event collection duration configuration
    */
    public long getEventCollectionDuration() {
        return eventCollectionDuration;
    }

    /*
       Read byte capacity configuration
    */
    public long getByteCapacity() {
        return byteCapacity;
    }

    /*
       Read the event count configuration
    */
    public int getEeventCount() {
        return eventCount;
    }
}
