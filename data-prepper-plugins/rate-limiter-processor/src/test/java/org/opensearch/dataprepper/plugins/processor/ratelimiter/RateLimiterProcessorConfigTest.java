/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.ratelimiter;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class RateLimiterProcessorConfigTest {

    @Test
    void test_default_when_exceeds_is_drop() {
        final RateLimiterProcessorConfig config = new RateLimiterProcessorConfig();
        assertThat(config.getWhenExceeds(), equalTo(RateLimiterMode.DROP));
    }

    @Test
    void test_default_counter_retention_seconds_is_60() {
        final RateLimiterProcessorConfig config = new RateLimiterProcessorConfig();
        assertThat(config.getCounterRetentionSeconds(), equalTo(60));
    }

}
