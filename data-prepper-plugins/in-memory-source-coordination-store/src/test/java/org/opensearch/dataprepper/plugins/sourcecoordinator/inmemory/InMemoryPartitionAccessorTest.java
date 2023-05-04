/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class InMemoryPartitionAccessorTest {

    private InMemoryPartitionAccessor objectUnderTest;

    @BeforeEach
    void setup() {
        objectUnderTest = new InMemoryPartitionAccessor();
    }

    @Test
    void getItem_with_no_item_returns_empty_optional() {
        final String partitionKey = UUID.randomUUID().toString();
        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getItem(partitionKey);

        assertThat(resultItem.isEmpty(), equalTo(true));
    }

    @Test
    void getNextItem_returns_empty_optional_when_no_partitions_are_queued() {
        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getNextItem();

        assertThat(resultItem.isEmpty(), equalTo(true));
    }

    @Test
    void queuePartition_followed_by_get_partition_returns_expected_partition() {
        final String partitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);

        objectUnderTest.queuePartition(item);

        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getItem(partitionKey);

        assertThat(resultItem.isPresent(), equalTo(true));
        assertThat(resultItem.get(), equalTo(item));
    }

    @Test
    void queuePartitions_followed_by_get_next_item_returns_items_in_order_they_were_queued() {
        final String partitionKey = UUID.randomUUID().toString();
        final String secondPartitionKey = UUID.randomUUID().toString();

        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);

        final InMemorySourcePartitionStoreItem secondItem = mock(InMemorySourcePartitionStoreItem.class);
        given(secondItem.getSourcePartitionKey()).willReturn(secondPartitionKey);

        objectUnderTest.queuePartition(item);
        objectUnderTest.queuePartition(secondItem);

        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getNextItem();

        assertThat(resultItem.isPresent(), equalTo(true));
        assertThat(resultItem.get(), equalTo(item));

        final Optional<SourcePartitionStoreItem> secondResultItem = objectUnderTest.getNextItem();

        assertThat(secondResultItem.isPresent(), equalTo(true));
        assertThat(secondResultItem.get(), equalTo(secondItem));
    }

    @Test
    void getNextItem_with_closed_item_that_has_not_reopened_moves_item_to_back_of_queue_and_is_processed_when_reached_again() {
        final String partitionKey = UUID.randomUUID().toString();
        final String secondPartitionKey = UUID.randomUUID().toString();

        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);
        given(item.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.CLOSED);
        given(item.getReOpenAt()).willReturn(Instant.now().plusSeconds(10))
                .willReturn(Instant.now().plusSeconds(10))
                .willReturn(Instant.now().minusSeconds(10))
                .willReturn(Instant.now().minusSeconds(10));

        final InMemorySourcePartitionStoreItem secondItem = mock(InMemorySourcePartitionStoreItem.class);
        given(secondItem.getSourcePartitionKey()).willReturn(secondPartitionKey);

        objectUnderTest.queuePartition(item);
        objectUnderTest.queuePartition(secondItem);

        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getNextItem();

        assertThat(resultItem.isPresent(), equalTo(true));
        assertThat(resultItem.get(), equalTo(secondItem));

        final Optional<SourcePartitionStoreItem> secondResultItem = objectUnderTest.getNextItem();

        assertThat(secondResultItem.isPresent(), equalTo(true));
        assertThat(secondResultItem.get(), equalTo(item));
    }

    @Test
    void updateItem_that_does_not_exist_does_not_update_or_queue_item() {
        final String partitionKey = UUID.randomUUID().toString();

        final InMemorySourcePartitionStoreItem updateItem = mock(InMemorySourcePartitionStoreItem.class);
        given(updateItem.getSourcePartitionKey()).willReturn(partitionKey);

        objectUnderTest.updateItem(updateItem);

        final Optional<SourcePartitionStoreItem> emptyItem = objectUnderTest.getNextItem();
        assertThat(emptyItem.isEmpty(), equalTo(true));

    }

    @ParameterizedTest
    @ValueSource(strings = {"CLOSED", "UNASSIGNED"})
    void updateItem_with_unassigned_or_closed_status_requeues_the_item(final String status) {
        final String partitionKey = UUID.randomUUID().toString();

        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);

        objectUnderTest.queuePartition(item);

        final Optional<SourcePartitionStoreItem> originalItem = objectUnderTest.getNextItem();

        assertThat(originalItem.isPresent(), equalTo(true));
        assertThat(originalItem.get(), equalTo(item));

        final InMemorySourcePartitionStoreItem updateItem = mock(InMemorySourcePartitionStoreItem.class);
        given(updateItem.getSourcePartitionKey()).willReturn(partitionKey);
        given(updateItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.valueOf(status));

        objectUnderTest.updateItem(updateItem);

        final Optional<SourcePartitionStoreItem> updatedItem = objectUnderTest.getNextItem();
        assertThat(updatedItem.isPresent(), equalTo(true));
        assertThat(updatedItem.get(), equalTo(updateItem));
    }

}
