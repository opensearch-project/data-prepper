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

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class RateLimiterProcessorConfigTest {

    @Test
    void test_default_config() {
        final RateLimiterProcessorConfig config = new RateLimiterProcessorConfig();
        assertThat(config.getWhenExceeds(), equalTo(RateLimiterMode.DROP));
        assertThat(config.getLimitWhen(), nullValue());
        assertThat(config.getCounterRetention(),equalTo(Duration.ofSeconds(60)));
    }
}
