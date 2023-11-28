/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
public class InMemorySourceCoordinationStoreTest {

    @Mock
    private InMemoryPartitionAccessor inMemoryPartitionAccessor;

    private InMemorySourceCoordinationStore createObjectUnderTest() {
        return new InMemorySourceCoordinationStore(inMemoryPartitionAccessor);
    }

    @Test
    void getSourcePartitionItem_returns_result_from_inMemoryPartitionAccessor_getItem() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();

        final SourcePartitionStoreItem item = mock(SourcePartitionStoreItem.class);
        given(inMemoryPartitionAccessor.getItem(sourceIdentifier, partitionKey)).willReturn(Optional.of(item));

        final InMemorySourceCoordinationStore objectUnderTest = createObjectUnderTest();

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getSourcePartitionItem(sourceIdentifier, partitionKey);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(item));
    }

    @Test
    void tryAcquireAvailablePartition_returns_empty_optional_when_no_item_is_available() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        given(inMemoryPartitionAccessor.getNextItem()).willReturn(Optional.empty());

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(sourceIdentifier, ownerId, ownershipTimeout);
        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void tryAcquireAvailablePartition_gets_item_from_inMemoryPartitionAccessor_and_modifies_it_correctly() throws InterruptedException {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);
        final Instant now = Instant.now();

        final SourcePartitionStoreItem item = new InMemorySourcePartitionStoreItem();

        given(inMemoryPartitionAccessor.getNextItem()).willReturn(Optional.of(item));

        final InMemorySourceCoordinationStore objectUnderTest = createObjectUnderTest();
        final Optional<SourcePartitionStoreItem> result = objectUnderTest.tryAcquireAvailablePartition(sourceIdentifier, ownerId, ownershipTimeout);
        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(item));
        assertThat(result.get().getSourcePartitionStatus(), equalTo(SourcePartitionStatus.ASSIGNED));
        assertThat(result.get().getPartitionOwnershipTimeout(), greaterThanOrEqualTo(now.plus(ownershipTimeout)));
        assertThat(result.get().getPartitionOwner(), equalTo(ownerId));

    }

    @Test
    void tryUpdateSourcePartitionItem_calls_updateItem_of_InMemoryPartitionAccessor() {
        final SourcePartitionStoreItem item = mock(InMemorySourcePartitionStoreItem.class);

        doNothing().when(inMemoryPartitionAccessor).updateItem((InMemorySourcePartitionStoreItem) item);

        createObjectUnderTest().tryUpdateSourcePartitionItem(item);
    }

    @Test
    void tryCreatePartitionItem_for_item_that_exists_does_not_queuePartition_and_returns_false() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();
        final SourcePartitionStatus status = SourcePartitionStatus.UNASSIGNED;
        final Long closedCount = 0L;
        final String partitionProgressState = UUID.randomUUID().toString();


        given(inMemoryPartitionAccessor.getItem(sourceIdentifier, partitionKey)).willReturn(Optional.of(mock(SourcePartitionStoreItem.class)));
        final InMemorySourceCoordinationStore objectUnderTest = createObjectUnderTest();

        final boolean created = objectUnderTest.tryCreatePartitionItem(sourceIdentifier, partitionKey, status, closedCount, partitionProgressState, false);
        assertThat(created, equalTo(false));
        verify(inMemoryPartitionAccessor, never()).queuePartition(any());
    }

    @Test
    void tryCreatePartitionItem_for_item_that_does_not_exist_queues_that_partition() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String partitionKey = UUID.randomUUID().toString();
        final SourcePartitionStatus status = SourcePartitionStatus.UNASSIGNED;
        final Long closedCount = 0L;
        final String partitionProgressState = UUID.randomUUID().toString();

        final ArgumentCaptor<InMemorySourcePartitionStoreItem> argumentCaptor = ArgumentCaptor.forClass(InMemorySourcePartitionStoreItem.class);


        given(inMemoryPartitionAccessor.getItem(sourceIdentifier, partitionKey)).willReturn(Optional.empty());
        doNothing().when(inMemoryPartitionAccessor).queuePartition(argumentCaptor.capture());
        final InMemorySourceCoordinationStore objectUnderTest = createObjectUnderTest();

        final boolean created = objectUnderTest.tryCreatePartitionItem(sourceIdentifier, partitionKey, status, closedCount, partitionProgressState, false);
        assertThat(created, equalTo(true));

        final InMemorySourcePartitionStoreItem createdItem = argumentCaptor.getValue();
        assertThat(createdItem, notNullValue());
        assertThat(createdItem.getSourceIdentifier(), equalTo(sourceIdentifier));
        assertThat(createdItem.getSourcePartitionKey(), equalTo(partitionKey));
        assertThat(createdItem.getSourcePartitionStatus(), equalTo(status));
        assertThat(createdItem.getPartitionProgressState(), equalTo(partitionProgressState));
        assertThat(createdItem.getClosedCount(), equalTo(closedCount));
    }
}
