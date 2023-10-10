/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class QueuedPartitionsItemTest {

    @Test
    void queued_partitions_items_with_null_sortedTimestamp_considers_items_equal() {
        final InMemoryPartitionAccessor.QueuedPartitionsItem firstItem = new InMemoryPartitionAccessor.QueuedPartitionsItem(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                null
        );

        final InMemoryPartitionAccessor.QueuedPartitionsItem secondItem = new InMemoryPartitionAccessor.QueuedPartitionsItem(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                null
        );

        assertThat(firstItem.compareTo(secondItem), equalTo(0));
        assertThat(secondItem.compareTo(firstItem), equalTo(0));
    }

    @Test
    void queued_partitions_items_with_one_null_sortedTimestamp_and_one_non_null_considers_considers_item_with_null_sortedtimestamp_to_be_less_than() {
        final InMemoryPartitionAccessor.QueuedPartitionsItem firstItem = new InMemoryPartitionAccessor.QueuedPartitionsItem(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                Instant.now()
        );

        final InMemoryPartitionAccessor.QueuedPartitionsItem secondItem = new InMemoryPartitionAccessor.QueuedPartitionsItem(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                null
        );

        assertThat(firstItem.compareTo(secondItem), equalTo(1));
        assertThat(secondItem.compareTo(firstItem), equalTo(-1));
    }

    @Test
    void queued_partitions_item_compares_based_on_sorted_timestamp() throws InterruptedException {
        final InMemoryPartitionAccessor.QueuedPartitionsItem firstItem = new InMemoryPartitionAccessor.QueuedPartitionsItem(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                Instant.now()
        );
        Thread.sleep(100);
        final Instant now = Instant.now();

        final InMemoryPartitionAccessor.QueuedPartitionsItem secondItem = new InMemoryPartitionAccessor.QueuedPartitionsItem(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                now
        );
        final InMemoryPartitionAccessor.QueuedPartitionsItem thirdItem = new InMemoryPartitionAccessor.QueuedPartitionsItem(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                now
        );



        assertThat(firstItem.compareTo(secondItem), lessThan(0));
        assertThat(secondItem.compareTo(firstItem), greaterThan(0));
        assertThat(secondItem.compareTo(thirdItem), equalTo(0));
    }
}
