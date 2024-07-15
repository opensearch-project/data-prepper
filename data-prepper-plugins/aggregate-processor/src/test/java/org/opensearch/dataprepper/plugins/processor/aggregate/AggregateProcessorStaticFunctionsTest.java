/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.Duration;

public class AggregateProcessorStaticFunctionsTest {
    @Test
    public void testConvertObjectToInstant() {
        Instant now = Instant.now();
        assertThat(AggregateProcessor.convertObjectToInstant(now), equalTo(now));
        String nowStr = now.toString();
        long nowSeconds = now.getEpochSecond();
        long nowMillis = now.toEpochMilli();
        int nowNanos = now.getNano();
        double nowDouble = nowSeconds+(double)nowNanos/1000_000_000;
        assertThat(AggregateProcessor.convertObjectToInstant(nowStr), equalTo(now));
        assertThat(AggregateProcessor.convertObjectToInstant(nowSeconds), equalTo(Instant.ofEpochSecond(nowSeconds)));
        assertThat(AggregateProcessor.convertObjectToInstant(nowMillis), equalTo(Instant.ofEpochMilli(nowMillis)));
        Duration tolerance = Duration.ofNanos(1000);
        assertTrue((Duration.between(AggregateProcessor.convertObjectToInstant(nowDouble), Instant.ofEpochSecond(nowSeconds, nowNanos))).abs().compareTo(tolerance) <= 0);
    }

    @Test
    public void testGetTimeNanos() {
        Instant now = Instant.now();
        assertThat(AggregateProcessor.getTimeNanos(now) / 1000_000_000, equalTo(now.getEpochSecond()));
        assertThat(AggregateProcessor.getTimeNanos(now) % 1000_000_000, equalTo((long)now.getNano()));
    }
}

