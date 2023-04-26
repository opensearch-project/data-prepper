/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;

class ThresholdOptionsTest {
    private static final String DEFAULT_BYTE_CAPACITY = "50mb";
    private static final int DEFAULT_EVENT_COUNT = 0;

    @Test
    void default_byte_capacity_test() {
        assertThat(new ThresholdOptions().getMaximumSize().getBytes(),
                equalTo(ByteCount.parse(DEFAULT_BYTE_CAPACITY).getBytes()));
    }

    @Test
    void get_event_collection_duration_test() {
        assertThat(new ThresholdOptions().getEventCollectTimeOut(), equalTo(null));
    }

    @Test
    void get_event_count_test() {
        assertThat(new ThresholdOptions().getEventCount(), equalTo(DEFAULT_EVENT_COUNT));
    }
}