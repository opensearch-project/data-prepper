/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Determines the shard (partition) for a given document and index.
 * Uses the same Murmur3 hash algorithm as OpenSearch to ensure documents
 * are routed to the correct shard.
 */
public class IndexRouter {
    private static final Logger LOG = LoggerFactory.getLogger(IndexRouter.class);

    private final IndexShardProvider indexShardProvider;

    private String indexName;
    private boolean initialized;

    public IndexRouter(final IndexShardProvider indexShardProvider) {
        this.indexShardProvider = indexShardProvider;
    }

    public void initialize(final String indexName) throws IOException {
        this.indexName = indexName;
        indexShardProvider.getNumberOfShards(indexName);
        final int routingPartitionSize = indexShardProvider.getRoutingPartitionSize(indexName);
        if (routingPartitionSize > 1) {
            throw new UnsupportedOperationException(String.format(
                    "routing-partitioned indices (routing_partition_size=%d) are not supported by pull-based ingestion; "
                            + "shard routing for index '%s' would require the document id to compute the partition offset",
                    routingPartitionSize, indexName));
        }
        initialized = true;
    }

    public int getNumberOfShards() throws IOException {
        if (!initialized) {
            throw new IllegalStateException("IndexRouter has not been initialized");
        }
        return indexShardProvider.getNumberOfShards(indexName);
    }

    public int getShardForRouting(final String routingValue) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("IndexRouter has not been initialized");
        }
        final int numberOfShards = indexShardProvider.getNumberOfShards(indexName);
        final int routingNumShards = indexShardProvider.getNumberOfRoutingShards(indexName);
        if (routingNumShards < numberOfShards) {
            throw new IllegalStateException(String.format(
                    "number_of_routing_shards (%d) must be >= number_of_shards (%d) for index '%s'",
                    routingNumShards, numberOfShards, indexName));
        }
        final int routingFactor = routingNumShards / numberOfShards;
        return calculateShard(routingValue, routingNumShards, routingFactor);
    }

    static int calculateShard(final String routingValue, final int routingNumShards, final int routingFactor) {
        final int hash = murmur3Hash(routingValue);
        return Math.floorMod(hash, routingNumShards) / routingFactor;
    }

    /**
     * Murmur3 hash matching OpenSearch's Murmur3HashFunction.hash(String).
     * 32-bit Murmur3 with seed 0, operating on the UTF-16 little-endian bytes of the input
     * (low byte then high byte for each char), the same byte layout OpenSearch hashes.
     * See https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/cluster/routing/Murmur3HashFunction.java
     */
    static int murmur3Hash(final String routing) {
        final byte[] bytes = new byte[routing.length() * 2];
        for (int i = 0; i < routing.length(); i++) {
            final char c = routing.charAt(i);
            bytes[i * 2] = (byte) c;
            bytes[i * 2 + 1] = (byte) (c >>> 8);
        }
        return murmur3_32(bytes, 0, bytes.length, 0);
    }

    private static int murmur3_32(final byte[] data, final int offset, final int length, final int seed) {
        int h = seed;
        final int nblocks = length >> 2;

        for (int i = 0; i < nblocks; i++) {
            final int idx = offset + (i << 2);
            int k = (data[idx] & 0xff)
                    | ((data[idx + 1] & 0xff) << 8)
                    | ((data[idx + 2] & 0xff) << 16)
                    | ((data[idx + 3] & 0xff) << 24);

            k *= 0xcc9e2d51;
            k = Integer.rotateLeft(k, 15);
            k *= 0x1b873593;

            h ^= k;
            h = Integer.rotateLeft(h, 13);
            h = h * 5 + 0xe6546b64;
        }

        final int tailOffset = offset + (nblocks << 2);
        int k = 0;
        switch (length - (nblocks << 2)) {
            case 3:
                k ^= (data[tailOffset + 2] & 0xff) << 16;
            case 2:
                k ^= (data[tailOffset + 1] & 0xff) << 8;
            case 1:
                k ^= (data[tailOffset] & 0xff);
                k *= 0xcc9e2d51;
                k = Integer.rotateLeft(k, 15);
                k *= 0x1b873593;
                h ^= k;
        }

        h ^= length;
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;

        return h;
    }
}
