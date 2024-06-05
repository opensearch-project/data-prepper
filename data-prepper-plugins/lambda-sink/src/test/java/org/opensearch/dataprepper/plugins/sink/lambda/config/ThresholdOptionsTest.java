/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.lambda.config;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class ThresholdOptionsTest {
    private static final String DEFAULT_BYTE_CAPACITY = "6mb";
    private static final int DEFAULT_EVENT_COUNT = 0;

    @Test
    void test_default_byte_capacity_test() {
        assertThat(new ThresholdOptions().getMaximumSize().getBytes(),
                equalTo(ByteCount.parse(DEFAULT_BYTE_CAPACITY).getBytes()));
    }

    @Test
    void test_get_event_collection_duration_test() {
        assertThat(new ThresholdOptions().getEventCollectTimeOut(), equalTo(null));
    }

    @Test
    void test_get_event_count_test() {
        assertThat(new ThresholdOptions().getEventCount(), equalTo(DEFAULT_EVENT_COUNT));
    }
}