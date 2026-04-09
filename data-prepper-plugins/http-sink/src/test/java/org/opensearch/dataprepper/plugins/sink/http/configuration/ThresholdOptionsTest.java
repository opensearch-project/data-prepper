/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.configuration;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class ThresholdOptionsTest {
    private static final String DEFAULT_BYTE_CAPACITY = "50mb";
    private static final int DEFAULT_EVENT_COUNT = 100;
    private static final Duration DEFAULT_EVENT_COLLECT_TIMEOUT = Duration.ofSeconds(10);

    @Test
    void default_byte_capacity_test() {
        MatcherAssert.assertThat(new ThresholdOptions().getMaxRequestSize().getBytes(),
                equalTo(ByteCount.parse(DEFAULT_BYTE_CAPACITY).getBytes()));
    }

    @Test
    void get_event_collection_duration_test() {
        assertThat(new ThresholdOptions().getFlushTimeOut(), equalTo(DEFAULT_EVENT_COLLECT_TIMEOUT));
    }

    @Test
    void get_event_count_test() {
        assertThat(new ThresholdOptions().getMaxEvents(), equalTo(DEFAULT_EVENT_COUNT));
    }
}