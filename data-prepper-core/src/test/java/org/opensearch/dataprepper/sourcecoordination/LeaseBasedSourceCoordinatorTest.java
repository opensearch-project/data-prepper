/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.parser.model.SourceCoordinationConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.opensearch.dataprepper.sourcecoordination.LeaseBasedSourceCoordinator.DEFAULT_LEASE_TIMEOUT;

@ExtendWith(MockitoExtension.class)
public class LeaseBasedSourceCoordinatorTest {

    @Mock
    private SourceCoordinationStore sourceCoordinationStore;

    @Mock
    private SourceCoordinationConfig sourceCoordinationConfig;

    @Mock
    private PartitionManager<String> partitionManager;

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    private String ownerPrefix;

    @BeforeEach
    void setup() {
        ownerPrefix = UUID.randomUUID().toString();
    }

    private SourceCoordinator<String> createObjectUnderTest() {
        return new LeaseBasedSourceCoordinator<>(String.class, sourceCoordinationStore, sourceCoordinationConfig, partitionManager, ownerPrefix);
    }

    @Test
    void getNextPartition_calls_supplier_and_creates_partition_with_non_existing_item_when_partition_exists_and_is_created_successfully() {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Supplier<List<PartitionIdentifier>> partitionCreationSupplier = () -> List.of(partitionIdentifier);

        given(sourceCoordinationStore.tryAcquireAvailablePartition()).willReturn(Optional.empty()).willReturn( Optional.empty());
        given(sourceCoordinationStore.getSourcePartitionItem(partitionIdentifier.getPartitionKey())).willReturn(Optional.empty());
        given(sourceCoordinationStore.tryCreatePartitionItem(partitionIdentifier.getPartitionKey(), SourcePartitionStatus.UNASSIGNED, 0L, null)).willReturn(true);

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(partitionCreationSupplier);

        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void getNextPartition_calls_supplier_which_returns_existing_partition_does_not_create_the_existing_partition() {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Supplier<List<PartitionIdentifier>> partitionCreationSupplier = () -> List.of(partitionIdentifier);

        given(sourceCoordinationStore.tryAcquireAvailablePartition()).willReturn(Optional.empty()).willReturn( Optional.empty());
        given(sourceCoordinationStore.getSourcePartitionItem(partitionIdentifier.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(partitionCreationSupplier);

        assertThat(result.isEmpty(), equalTo(true));

        verify(sourceCoordinationStore, never()).tryCreatePartitionItem(anyString(), any(), anyLong(), anyString());
    }

    @Test
    void getNextPartition_where_with_non_existing_item_and_create_attempt_fails_will_do_nothing() {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Supplier<List<PartitionIdentifier>> partitionCreationSupplier = () -> List.of(partitionIdentifier);

        given(sourceCoordinationStore.tryAcquireAvailablePartition()).willReturn(Optional.empty()).willReturn( Optional.empty());
        given(sourceCoordinationStore.getSourcePartitionItem(partitionIdentifier.getPartitionKey())).willReturn(Optional.empty());
        given(sourceCoordinationStore.tryCreatePartitionItem(partitionIdentifier.getPartitionKey(), SourcePartitionStatus.UNASSIGNED, 0L, null)).willReturn(false);

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(partitionCreationSupplier);

        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void getNextPartition_that_has_active_partition_returns_that_SourcePartition() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                        .withPartitionKey(UUID.randomUUID().toString())
                        .withPartitionState(null)
                        .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(Collections::emptyList);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get().getPartitionKey(), equalTo(sourcePartition.getPartitionKey()));
        assertThat(result.get().getPartitionState(), equalTo(null));

        verifyNoInteractions(sourceCoordinationStore);
    }

    @Test
    void getNextPartition_with_no_active_partition_and_unsuccessful_tryAcquireAvailablePartition_returns_empty_Optional() {
        given(partitionManager.getActivePartition()).willReturn(Optional.empty());
        given(sourceCoordinationStore.tryAcquireAvailablePartition()).willReturn(Optional.empty()).willReturn(Optional.empty());


        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(Collections::emptyList);

        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void getNextPartition_with_no_active_partition_and_successful_tryAcquireAvailablePartition_returns_expected_SourcePartition() {
        given(partitionManager.getActivePartition()).willReturn(Optional.empty());
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(UUID.randomUUID().toString());
        given(sourcePartitionStoreItem.getPartitionProgressState()).willReturn(UUID.randomUUID().toString());
        given(sourceCoordinationStore.tryAcquireAvailablePartition()).willReturn(Optional.of(sourcePartitionStoreItem));

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(Collections::emptyList);


        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get().getPartitionKey(), equalTo(sourcePartitionStoreItem.getSourcePartitionKey()));
        assertThat(result.get().getPartitionState(), equalTo(sourcePartitionStoreItem.getPartitionProgressState()));

        verify(partitionManager).setActivePartition(result.get());
        verify(sourceCoordinationStore, never()).getSourcePartitionItem(anyString());
        verify(sourceCoordinationStore, never()).tryCreatePartitionItem(anyString(), any(), anyLong(), anyString());
    }

    @Test
    void completePartition_with_partitionKey_that_is_not_owned_throws_partition_not_owned_exception() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().completePartition(UUID.randomUUID().toString()));
    }

    @Test
    void completePartition_with_owned_partition_key_and_no_store_item_throws_PartitionNotFoundException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        assertThrows(PartitionNotFoundException.class, () -> createObjectUnderTest().completePartition(sourcePartition.getPartitionKey()));
    }

    @Test
    void completePartition_with_owned_partition_key_and_existing_store_item_with_invalid_owner_throws_PartitionNotOwnedException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(UUID.randomUUID().toString());
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(sourcePartition.getPartitionKey());
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().completePartition(sourcePartition.getPartitionKey()));

        verify(partitionManager).removeActivePartition();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void completePartition_with_owned_partition_and_existing_store_item_with_valid_owner_returns_expected_result(final boolean updatedItemSuccessfully) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(ownerPrefix + ":" + InetAddress.getLocalHost().getHostName());

        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            createObjectUnderTest().completePartition(sourcePartition.getPartitionKey());

            verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
            verify(sourcePartitionStoreItem).setReOpenAt(null);
            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
            verify(sourcePartitionStoreItem).setPartitionOwner(null);

            verify(partitionManager).removeActivePartition();
        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            assertThrows(PartitionUpdateException.class, () -> createObjectUnderTest().completePartition(sourcePartition.getPartitionKey()));
        }
    }

    @Test
    void closePartition_with_partitionKey_that_is_not_owned_throws_partition_not_owned_exception() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().closePartition(UUID.randomUUID().toString(), Duration.ofMinutes(2), 1));
    }

    @Test
    void closePartition_with_owned_partition_key_and_no_store_item_throws_PartitionNotFoundException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        assertThrows(PartitionNotFoundException.class, () -> createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), 1));
    }

    @Test
    void closePartition_with_owned_partition_key_and_existing_store_item_with_invalid_owner_throws_PartitionNotOwnedException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(UUID.randomUUID().toString());
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(sourcePartition.getPartitionKey());
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), 1));

        verify(partitionManager).removeActivePartition();
    }

    @ParameterizedTest
    @MethodSource("getClosedCountArgs")
    void closePartition_with_owned_partition_and_existing_store_item_with_valid_owner_and_below_max_closed_count_returns_expected_result(final boolean updatedItemSuccessfully, final Long closedCount) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(ownerPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourcePartitionStoreItem.getClosedCount()).willReturn(closedCount);
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        final int maxClosedCount = 2;
        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), maxClosedCount);

            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
            verify(sourcePartitionStoreItem).setPartitionOwner(null);

            if (closedCount >= maxClosedCount) {
                verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
            } else {
                verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
                verify(sourcePartitionStoreItem).setReOpenAt(any(Instant.class));
                verify(sourcePartitionStoreItem).setClosedCount(closedCount + 1L);
            }

            verify(partitionManager).removeActivePartition();
        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            assertThrows(PartitionUpdateException.class, () -> createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), maxClosedCount));
        }
    }

    @Test
    void savePartitionProgressState_with_partitionKey_that_is_not_owned_throws_partition_not_owned_exception() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().saveProgressStateForPartition(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    }

    @Test
    void savePartitionProgressState_with_owned_partition_key_and_no_store_item_throws_PartitionNotFoundException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        assertThrows(PartitionNotFoundException.class, () -> createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), UUID.randomUUID().toString()));
    }

    @Test
    void saveProgressStateForPartition_with_owned_partition_key_and_existing_store_item_with_invalid_owner_throws_PartitionNotOwnedException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(UUID.randomUUID().toString());
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(sourcePartition.getPartitionKey());
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), UUID.randomUUID().toString()));

        verify(partitionManager).removeActivePartition();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void saveProgressStateForPartition_with_owned_partition_and_existing_store_item_with_valid_owner_returns_expected_result(final boolean updatedItemSuccessfully) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        final Instant beforeSave = Instant.now();
        final String newProgressState = UUID.randomUUID().toString();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(ownerPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), newProgressState);

            final ArgumentCaptor<Instant> argumentCaptorForPartitionOwnershipTimeout = ArgumentCaptor.forClass(Instant.class);
            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(argumentCaptorForPartitionOwnershipTimeout.capture());
            final Instant newPartitionOwnershipTimeout = argumentCaptorForPartitionOwnershipTimeout.getValue();
            assertThat(newPartitionOwnershipTimeout.isAfter(beforeSave.plus(DEFAULT_LEASE_TIMEOUT)), equalTo(true));

            verify(sourcePartitionStoreItem).setPartitionProgressState(newProgressState);
        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            assertThrows(PartitionUpdateException.class, () -> createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), newProgressState));
        }
    }

    @Test
    void giveUpPartitions_with_active_partitionKey_that_does_not_exist_in_the_store_removes_the_active_partition() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        createObjectUnderTest().giveUpPartitions();

        verify(partitionManager).removeActivePartition();
        verifyNoInteractions(sourcePartitionStoreItem);
        verifyNoMoreInteractions(sourceCoordinationStore);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void giveUpPartitions_with_owned_partition_and_existing_store_item_with_valid_owner_returns_expected_result(final boolean updatedItemSuccessfully) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(partitionManager.getActivePartition()).willReturn(Optional.of(sourcePartition));
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(ownerPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            createObjectUnderTest().giveUpPartitions();

            verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
            verify(sourcePartitionStoreItem).setPartitionOwner(null);
            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);

            verify(partitionManager).removeActivePartition();
        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            assertThrows(PartitionUpdateException.class, () -> createObjectUnderTest().giveUpPartitions());
        }
    }

    static Stream<Object[]> getClosedCountArgs() {
        return Stream.of(
                new Object[]{true, 0L},
                new Object[]{false, 2L},
                new Object[]{true, 3L},
                new Object[]{false, 1L}
        );
    }
}
