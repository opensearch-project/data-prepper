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
import static org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory.InMemoryPartitionAccessor.GLOBAL_STATE_ITEM_SUFFIX;

public class InMemoryPartitionAccessorTest {

    private InMemoryPartitionAccessor objectUnderTest;

    @BeforeEach
    void setup() {
        objectUnderTest = new InMemoryPartitionAccessor();
    }

    @Test
    void getItem_with_global_store_item_that_does_not_exist_returns_empty_optional() {
        final String sourceIdentifier = UUID.randomUUID() + GLOBAL_STATE_ITEM_SUFFIX;
        final String partitionKey = UUID.randomUUID().toString();

        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);

        assertThat(resultItem.isEmpty(), equalTo(true));
    }
    @Test
    void getItem_with_no_sourceIdentifier_returns_empty_optional() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();
        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);

        assertThat(resultItem.isEmpty(), equalTo(true));
    }

    @Test
    void getNextItem_returns_empty_optional_when_no_partitions_are_queued() {
        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getNextItem();

        assertThat(resultItem.isEmpty(), equalTo(true));
    }

    @Test
    void queue_global_state_partition_followed_by_get_returns_global_state_item_and_does_not_queue_for_getNextItem() {
        final String sourceIdentifier = UUID.randomUUID() + GLOBAL_STATE_ITEM_SUFFIX;
        final String partitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);

        objectUnderTest.queuePartition(item);

        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);

        assertThat(resultItem.isPresent(), equalTo(true));
        assertThat(resultItem.get(), equalTo(item));

        final Optional<SourcePartitionStoreItem> emptyItem = objectUnderTest.getNextItem();
        assertThat(emptyItem.isEmpty(), equalTo(true));
    }

    @Test
    void queue_unassigned_partitions_followed_by_get_partitions_returns_expected_partitions() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);
        given(item.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);

        final String secondPartitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem secondItem = mock(InMemorySourcePartitionStoreItem.class);
        given(secondItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(secondItem.getSourcePartitionKey()).willReturn(secondPartitionKey);
        given(secondItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);

        objectUnderTest.queuePartition(item);
        objectUnderTest.queuePartition(secondItem);

        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);

        assertThat(resultItem.isPresent(), equalTo(true));
        assertThat(resultItem.get(), equalTo(item));

        final Optional<SourcePartitionStoreItem> acquiredItem = objectUnderTest.getNextItem();
        assertThat(acquiredItem.isPresent(), equalTo(true));
        assertThat(acquiredItem.get(), equalTo(item));

        final Optional<SourcePartitionStoreItem> secondAcquiredItem = objectUnderTest.getNextItem();
        assertThat(secondAcquiredItem.isPresent(), equalTo(true));
        assertThat(secondAcquiredItem.get(), equalTo(secondItem));

        final Optional<SourcePartitionStoreItem> emptyItem = objectUnderTest.getNextItem();
        assertThat(emptyItem.isEmpty(), equalTo(true));
    }

    @Test
    void queue_closed_partitions_followed_by_get_returns_expected_partitions() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);
        given(item.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.CLOSED);
        given(item.getReOpenAt()).willReturn(Instant.now().minusSeconds(60));

        final String secondPartitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem secondItem = mock(InMemorySourcePartitionStoreItem.class);
        given(secondItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(secondItem.getSourcePartitionKey()).willReturn(secondPartitionKey);
        given(secondItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.CLOSED);
        given(secondItem.getReOpenAt()).willReturn(Instant.now().minusSeconds(120));

        objectUnderTest.queuePartition(item);
        objectUnderTest.queuePartition(secondItem);

        final Optional<SourcePartitionStoreItem> acquiredItem = objectUnderTest.getNextItem();
        assertThat(acquiredItem.isPresent(), equalTo(true));
        assertThat(acquiredItem.get(), equalTo(secondItem));

        final Optional<SourcePartitionStoreItem> secondAcquiredItem = objectUnderTest.getNextItem();
        assertThat(secondAcquiredItem.isPresent(), equalTo(true));
        assertThat(secondAcquiredItem.get(), equalTo(item));

        final Optional<SourcePartitionStoreItem> emptyItem = objectUnderTest.getNextItem();
        assertThat(emptyItem.isEmpty(), equalTo(true));
    }

    @Test
    void queue_closed_then_unassigned_partitions_followed_by_get_returns_expected_partitions() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);
        given(item.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.CLOSED);
        given(item.getReOpenAt()).willReturn(Instant.now().minusSeconds(60));

        final String secondPartitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem secondItem = mock(InMemorySourcePartitionStoreItem.class);
        given(secondItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(secondItem.getSourcePartitionKey()).willReturn(secondPartitionKey);
        given(secondItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.CLOSED);
        given(secondItem.getReOpenAt()).willReturn(Instant.now().plusSeconds(120));

        final String thirdPartitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem thirdItem = mock(InMemorySourcePartitionStoreItem.class);
        given(thirdItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(thirdItem.getSourcePartitionKey()).willReturn(thirdPartitionKey);
        given(thirdItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);

        objectUnderTest.queuePartition(item);
        objectUnderTest.queuePartition(secondItem);
        objectUnderTest.queuePartition(thirdItem);

        final Optional<SourcePartitionStoreItem> acquiredItem = objectUnderTest.getNextItem();
        assertThat(acquiredItem.isPresent(), equalTo(true));
        assertThat(acquiredItem.get(), equalTo(thirdItem));

        final Optional<SourcePartitionStoreItem> secondAcquiredItem = objectUnderTest.getNextItem();
        assertThat(secondAcquiredItem.isPresent(), equalTo(true));
        assertThat(secondAcquiredItem.get(), equalTo(item));
    }

    @Test
    void updateItem_that_does_not_exist_does_not_update_or_queue_item() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();

        final InMemorySourcePartitionStoreItem updateItem = mock(InMemorySourcePartitionStoreItem.class);
        given(updateItem.getSourcePartitionKey()).willReturn(partitionKey);
        given(updateItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(updateItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);

        objectUnderTest.updateItem(updateItem);

        final Optional<SourcePartitionStoreItem> emptyItem = objectUnderTest.getNextItem();
        assertThat(emptyItem.isEmpty(), equalTo(true));
    }

    @Test
    void update_for_global_state_item_updates_that_item_and_does_not_queue() {
        final String sourceIdentifier = UUID.randomUUID() + GLOBAL_STATE_ITEM_SUFFIX;
        final String partitionKey = UUID.randomUUID().toString();
        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);

        objectUnderTest.queuePartition(item);

        final Optional<SourcePartitionStoreItem> resultItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);

        assertThat(resultItem.isPresent(), equalTo(true));
        assertThat(resultItem.get(), equalTo(item));

        final InMemorySourcePartitionStoreItem updateItem = mock(InMemorySourcePartitionStoreItem.class);
        given(updateItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(updateItem.getSourcePartitionKey()).willReturn(partitionKey);

        objectUnderTest.updateItem(updateItem);

        final Optional<SourcePartitionStoreItem> updatedItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);
        assertThat(updatedItem.isPresent(), equalTo(true));
        assertThat(updatedItem.get(), equalTo(updateItem));

        final Optional<SourcePartitionStoreItem> emptyItem = objectUnderTest.getNextItem();
        assertThat(emptyItem.isEmpty(), equalTo(true));
    }

    @Test
    void update_item_with_completed_status_adds_to_completed_set_but_is_not_requeued() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();

        final InMemorySourcePartitionStoreItem originalItem = mock(InMemorySourcePartitionStoreItem.class);
        given(originalItem.getSourcePartitionKey()).willReturn(partitionKey);
        given(originalItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(originalItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);

        objectUnderTest.queuePartition(originalItem);

        final Optional<SourcePartitionStoreItem> acquiredItem = objectUnderTest.getNextItem();
        assertThat(acquiredItem.isPresent(), equalTo(true));
        assertThat(acquiredItem.get(), equalTo(originalItem));

        final InMemorySourcePartitionStoreItem updateItem = mock(InMemorySourcePartitionStoreItem.class);
        given(updateItem.getSourcePartitionKey()).willReturn(partitionKey);
        given(updateItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(updateItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.COMPLETED);

        objectUnderTest.updateItem(updateItem);

        final Optional<SourcePartitionStoreItem> completedItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);
        assertThat(completedItem.isPresent(), equalTo(true));
        assertThat(completedItem.get().getSourceIdentifier(), equalTo(sourceIdentifier));
        assertThat(completedItem.get().getSourcePartitionKey(), equalTo(partitionKey));
        assertThat(completedItem.get().getSourcePartitionStatus(), equalTo(SourcePartitionStatus.COMPLETED));

        final Optional<SourcePartitionStoreItem> emptyItem = objectUnderTest.getNextItem();
        assertThat(emptyItem.isEmpty(), equalTo(true));

    }

    @ParameterizedTest
    @ValueSource(strings = {"CLOSED", "UNASSIGNED"})
    void updateItem_with_unassigned_or_closed_status_requeues_the_item(final String status) {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();

        final InMemorySourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);
        given(item.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(item.getSourcePartitionKey()).willReturn(partitionKey);
        given(item.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);

        objectUnderTest.queuePartition(item);

        final Optional<SourcePartitionStoreItem> originalItem = objectUnderTest.getNextItem();

        assertThat(originalItem.isPresent(), equalTo(true));
        assertThat(originalItem.get(), equalTo(item));

        final InMemorySourcePartitionStoreItem updateItem = mock(InMemorySourcePartitionStoreItem.class);
        given(updateItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(updateItem.getSourcePartitionKey()).willReturn(partitionKey);
        given(updateItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.valueOf(status));

        if (status.equals(SourcePartitionStatus.CLOSED.toString())) {
            given(updateItem.getReOpenAt()).willReturn(Instant.now().minusSeconds(120));
        }

        objectUnderTest.updateItem(updateItem);

        final Optional<SourcePartitionStoreItem> updatedItem = objectUnderTest.getNextItem();
        assertThat(updatedItem.isPresent(), equalTo(true));
        assertThat(updatedItem.get(), equalTo(updateItem));

        final Optional<SourcePartitionStoreItem> getUpdatedItem = objectUnderTest.getItem(sourceIdentifier, partitionKey);
        assertThat(getUpdatedItem.isPresent(), equalTo(true));
        assertThat(getUpdatedItem.get(), equalTo(updateItem));
    }

}
