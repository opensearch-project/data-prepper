/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IndexRouterTest {

    @Mock
    private IndexShardProvider indexShardProvider;

    private String indexName;
    private int shardCount;

    @BeforeEach
    void setUp() {
        indexName = UUID.randomUUID().toString();
        shardCount = 5;
    }

    private IndexRouter createObjectUnderTest() {
        return new IndexRouter(indexShardProvider);
    }

    @Test
    void initialize_fetches_shard_count_from_provider() throws IOException {
        when(indexShardProvider.getNumberOfShards(indexName)).thenReturn(shardCount);

        final IndexRouter objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(indexName);

        verify(indexShardProvider).getNumberOfShards(indexName);
        assertThat(objectUnderTest.getNumberOfShards(), equalTo(shardCount));
    }

    @Test
    void getNumberOfShards_throws_if_not_initialized() {
        assertThrows(IllegalStateException.class, () -> createObjectUnderTest().getNumberOfShards());
    }

    @Test
    void getShardForRouting_throws_if_not_initialized() {
        assertThrows(IllegalStateException.class, () -> createObjectUnderTest().getShardForRouting(UUID.randomUUID().toString()));
    }

    @Test
    void getShardForRouting_returns_valid_partition() throws IOException {
        when(indexShardProvider.getNumberOfShards(indexName)).thenReturn(shardCount);
        when(indexShardProvider.getNumberOfRoutingShards(indexName)).thenReturn(shardCount);
        final IndexRouter objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(indexName);

        for (final String id : List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString())) {
            final int shard = objectUnderTest.getShardForRouting(id);
            assertThat(shard, greaterThanOrEqualTo(0));
            assertThat(shard, lessThan(shardCount));
        }
    }

    @Test
    void getShardForRouting_is_deterministic() throws IOException {
        when(indexShardProvider.getNumberOfShards(indexName)).thenReturn(shardCount);
        when(indexShardProvider.getNumberOfRoutingShards(indexName)).thenReturn(shardCount);
        final IndexRouter objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(indexName);

        final String routingId = UUID.randomUUID().toString();
        assertThat(objectUnderTest.getShardForRouting(routingId),
                equalTo(objectUnderTest.getShardForRouting(routingId)));
    }

    @ParameterizedTest
    @CsvSource({
            "1, 0",
            "5, 4",
            "12, 9"
    })
    void getShardForRouting_known_values_for_abc123(final int shardCount, final int expectedShard) throws IOException {
        when(indexShardProvider.getNumberOfShards(indexName)).thenReturn(shardCount);
        when(indexShardProvider.getNumberOfRoutingShards(indexName)).thenReturn(shardCount);
        final IndexRouter objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(indexName);

        assertThat(objectUnderTest.getShardForRouting("abc123"), equalTo(expectedShard));
    }

    @Test
    void murmur3Hash_matches_opensearch_known_values() {
        assertThat(IndexRouter.murmur3Hash("hell"), equalTo(0x5a0cb7c3));
        assertThat(IndexRouter.murmur3Hash("hello"), equalTo(0xd7c31989));
        assertThat(IndexRouter.murmur3Hash("hello w"), equalTo(0x22ab2984));
        assertThat(IndexRouter.murmur3Hash("hello wo"), equalTo(0xdf0ca123));
        assertThat(IndexRouter.murmur3Hash("hello wor"), equalTo(0xe7744d61));
        assertThat(IndexRouter.murmur3Hash("The quick brown fox jumps over the lazy dog"), equalTo(0xe07db09c));
        assertThat(IndexRouter.murmur3Hash("The quick brown fox jumps over the lazy cog"), equalTo(0x4e63d2ad));
    }

    @ParameterizedTest
    @CsvSource({
            "abc123, 5, 1, 4",
            "abc123, 10, 2, 4",
            "abc123, 12, 1, 9",
            "abc123, 12, 4, 2",
            "hello, 5, 1, 1",
            "hello, 10, 2, 0",
            "hell, 12, 1, 3"
    })
    void calculateShard_matches_opensearch_scaled_formula(final String routingValue, final int routingNumShards,
                                                          final int routingFactor, final int expectedShard) {
        assertThat(IndexRouter.calculateShard(routingValue, routingNumShards, routingFactor), equalTo(expectedShard));
    }

    @Test
    void getShardForRouting_scales_by_routing_factor_when_routing_num_shards_exceeds_shards() throws IOException {
        when(indexShardProvider.getNumberOfShards(indexName)).thenReturn(5);
        when(indexShardProvider.getNumberOfRoutingShards(indexName)).thenReturn(10);
        final IndexRouter objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(indexName);

        assertThat(objectUnderTest.getShardForRouting("abc123"), equalTo(4));
    }

    @Test
    void getShardForRouting_calls_provider_each_time() throws IOException {
        when(indexShardProvider.getNumberOfShards(indexName)).thenReturn(shardCount);
        when(indexShardProvider.getNumberOfRoutingShards(indexName)).thenReturn(shardCount);
        final IndexRouter objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(indexName);

        objectUnderTest.getShardForRouting("abc123");
        objectUnderTest.getShardForRouting("abc123");

        verify(indexShardProvider, times(2)).getNumberOfRoutingShards(indexName);
    }
}
