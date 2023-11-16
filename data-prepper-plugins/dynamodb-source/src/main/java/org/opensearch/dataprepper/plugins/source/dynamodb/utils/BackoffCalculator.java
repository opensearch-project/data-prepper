/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.utils;

import org.opensearch.dataprepper.plugins.source.dynamodb.stream.StreamScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.math.LongMath.pow;
import static com.google.common.primitives.Longs.min;
import static java.lang.Math.max;

public class BackoffCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);

    private static final Random RANDOM = new Random();

    static final Duration STARTING_BACKOFF = Duration.ofMillis(500);
    static final Duration MAX_BACKOFF_WITH_SHARDS = Duration.ofSeconds(15);
    static final Duration MAX_BACKOFF_NO_SHARDS_ACQUIRED = Duration.ofSeconds(15);
    static final int BACKOFF_RATE = 2;
    static final Duration MAX_JITTER = Duration.ofSeconds(2);
    static final Duration MIN_JITTER = Duration.ofSeconds(-2);

    public long calculateBackoffToAcquireNextShard(final int noAvailableShardCount, final AtomicInteger shardsAcquired) {

        // When no shards are available to process we backoff exponentially based on how many consecutive attempts have been made without getting a shard
        // This limits calls to the coordination store
        if (noAvailableShardCount > 0) {
            if (noAvailableShardCount % 10 == 0) {
                LOG.info("No shards acquired after {} attempts", noAvailableShardCount);
            }

            final long jitterMillis = MIN_JITTER.toMillis() + RANDOM.nextInt((int) (MAX_JITTER.toMillis() - MIN_JITTER.toMillis() + 1));
            return max(1, min(STARTING_BACKOFF.toMillis() * pow(BACKOFF_RATE, noAvailableShardCount - 1) + jitterMillis, MAX_BACKOFF_NO_SHARDS_ACQUIRED.toMillis()));
        }

        // When shards are being acquired we backoff linearly based on how many shards this node is actively processing, to encourage a fast start but still a balance of shards between nodes
        return max(500, min(MAX_BACKOFF_WITH_SHARDS.toMillis(), shardsAcquired.get() * STARTING_BACKOFF.toMillis()));
    }
}
