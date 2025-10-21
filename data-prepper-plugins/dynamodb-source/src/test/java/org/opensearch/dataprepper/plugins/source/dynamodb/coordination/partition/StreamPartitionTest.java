package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class StreamPartitionTest {

   @Test
    void hashCode_returns_same_value_for_same_shardId() {
       final String shardId = UUID.randomUUID().toString();
       final StreamPartition firstPartition = new StreamPartition(UUID.randomUUID().toString(), shardId, Optional.of(mock(StreamProgressState.class)));
       final StreamPartition secondPartition = new StreamPartition(UUID.randomUUID().toString(), shardId, Optional.of(mock(StreamProgressState.class)));
        assertThat(firstPartition.hashCode(),
                equalTo(secondPartition.hashCode()));
    }

    @Test
    void hashCode_returns_different_value_for_different_shard_id() {
        final StreamPartition firstPartition = new StreamPartition(UUID.randomUUID().toString(), UUID.randomUUID().toString(), Optional.of(mock(StreamProgressState.class)));
        final StreamPartition secondPartition = new StreamPartition(UUID.randomUUID().toString(), UUID.randomUUID().toString(), Optional.of(mock(StreamProgressState.class)));
        assertThat(firstPartition.hashCode(),
                not(equalTo(secondPartition.hashCode())));
    }

    @Test
    void equals_returns_true_for_same_shard_id() {
        final String shardId = UUID.randomUUID().toString();
        final StreamPartition firstPartition = new StreamPartition(UUID.randomUUID().toString(), shardId, Optional.of(mock(StreamProgressState.class)));
        final StreamPartition secondPartition = new StreamPartition(UUID.randomUUID().toString(), shardId, Optional.of(mock(StreamProgressState.class)));
        assertThat(firstPartition.equals(secondPartition),
                equalTo(true));
    }

    @Test
    void equals_returns_false_for_different_shard_id() {
        final StreamPartition firstPartition = new StreamPartition(UUID.randomUUID().toString(), UUID.randomUUID().toString(), Optional.of(mock(StreamProgressState.class)));
        final StreamPartition secondPartition = new StreamPartition(UUID.randomUUID().toString(), UUID.randomUUID().toString(), Optional.of(mock(StreamProgressState.class)));
        assertThat(firstPartition.equals(secondPartition),
                equalTo(false));
    }
}
