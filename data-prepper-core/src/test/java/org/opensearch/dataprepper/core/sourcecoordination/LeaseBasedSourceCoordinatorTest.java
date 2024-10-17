/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.sourcecoordination;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.model.SourceCoordinationConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.SourceCoordinationStore;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.UninitializedSourceCoordinatorException;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.DEFAULT_LEASE_TIMEOUT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.FORCE_SUPPLIER_AFTER_DURATION;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.GLOBAL_STATE_TYPE;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.NO_PARTITIONS_ACQUIRED_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITIONS_ACQUIRED_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITIONS_CLOSED_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITIONS_COMPLETED_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITIONS_DELETED;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITION_CREATED_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITION_CREATION_SUPPLIER_INVOCATION_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITION_NOT_FOUND_ERROR_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITION_NOT_OWNED_ERROR_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITION_OWNERSHIP_GIVEN_UP_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITION_TYPE;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.PARTITION_UPDATE_ERROR_COUNT;
import static org.opensearch.dataprepper.core.sourcecoordination.LeaseBasedSourceCoordinator.SAVE_PROGRESS_STATE_INVOCATION_SUCCESS_COUNT;

@ExtendWith(MockitoExtension.class)
public class LeaseBasedSourceCoordinatorTest {

    @Mock
    private SourceCoordinationStore sourceCoordinationStore;

    @Mock
    private SourceCoordinationConfig sourceCoordinationConfig;

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter partitionCreationSupplierInvocationsCounter;

    @Mock
    private Counter partitionsCreatedCounter;

    @Mock
    private Counter noPartitionsAcquiredCounter;

    @Mock
    private Counter partitionsAcquiredCounter;

    @Mock
    private Counter partitionsCompletedCounter;

    @Mock
    private Counter partitionsClosedCounter;

    @Mock
    private Counter saveProgressStateInvocationSuccessCounter;

    @Mock
    private Counter partitionsGivenUpCounter;

    @Mock
    private Counter partitionNotFoundErrorCounter;

    @Mock
    private Counter partitionNotOwnedErrorCounter;

    @Mock
    private Counter saveStatePartitionUpdateErrorCounter;

    @Mock
    private Counter closePartitionUpdateErrorCounter;

    @Mock
    private Counter completePartitionUpdateErrorCounter;

    @Mock
    private Counter partitionsDeletedCounter;

    @Mock
    private SourcePartitionStoreItem globalStateForPartitionCreationItem;

    private String sourceIdentifier;
    private String sourceIdentifierWithPartitionPrefix;
    private String fullSourceIdentifierForPartition;
    private String fullSourceIdentifierForGlobalState;

    @BeforeEach
    void setup() {
        final String partitionPrefix = UUID.randomUUID().toString();
        sourceIdentifier = UUID.randomUUID().toString();
        sourceIdentifierWithPartitionPrefix = partitionPrefix + "|" + sourceIdentifier;

        fullSourceIdentifierForPartition = sourceIdentifierWithPartitionPrefix + "|" + PARTITION_TYPE;
        this.fullSourceIdentifierForGlobalState = sourceIdentifierWithPartitionPrefix + "|" + GLOBAL_STATE_TYPE;
        given(sourceCoordinationConfig.getPartitionPrefix()).willReturn(partitionPrefix);
        given(pluginMetrics.counter(PARTITION_CREATION_SUPPLIER_INVOCATION_COUNT)).willReturn(partitionCreationSupplierInvocationsCounter);
        given(pluginMetrics.counter(NO_PARTITIONS_ACQUIRED_COUNT)).willReturn(noPartitionsAcquiredCounter);
        given(pluginMetrics.counter(PARTITIONS_ACQUIRED_COUNT)).willReturn(partitionsAcquiredCounter);
        given(pluginMetrics.counter(PARTITION_CREATED_COUNT)).willReturn(partitionsCreatedCounter);
        given(pluginMetrics.counter(PARTITIONS_COMPLETED_COUNT)).willReturn(partitionsCompletedCounter);
        given(pluginMetrics.counter(PARTITIONS_CLOSED_COUNT)).willReturn(partitionsClosedCounter);
        given(pluginMetrics.counter(SAVE_PROGRESS_STATE_INVOCATION_SUCCESS_COUNT)).willReturn(saveProgressStateInvocationSuccessCounter);
        given(pluginMetrics.counter(PARTITION_OWNERSHIP_GIVEN_UP_COUNT)).willReturn(partitionsGivenUpCounter);
        given(pluginMetrics.counter(PARTITION_NOT_FOUND_ERROR_COUNT)).willReturn(partitionNotFoundErrorCounter);
        given(pluginMetrics.counter(PARTITION_NOT_OWNED_ERROR_COUNT)).willReturn(partitionNotOwnedErrorCounter);
        given(pluginMetrics.counter(PARTITION_UPDATE_ERROR_COUNT, "saveState")).willReturn(saveStatePartitionUpdateErrorCounter);
        given(pluginMetrics.counter(PARTITION_UPDATE_ERROR_COUNT, "close")).willReturn(closePartitionUpdateErrorCounter);
        given(pluginMetrics.counter(PARTITION_UPDATE_ERROR_COUNT, "complete")).willReturn(completePartitionUpdateErrorCounter);
        given(pluginMetrics.counter(PARTITIONS_DELETED)).willReturn(partitionsDeletedCounter);
    }

    private SourceCoordinator<String> createObjectUnderTest() {
        final SourceCoordinator<String> objectUnderTest = new LeaseBasedSourceCoordinator<>(String.class, sourceCoordinationStore, sourceCoordinationConfig, sourceIdentifier, pluginMetrics);
        doNothing().when(sourceCoordinationStore).initializeStore();
        given(sourceCoordinationStore.tryCreatePartitionItem(
                fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS,
                SourcePartitionStatus.UNASSIGNED, 0L, null, false)).willReturn(true);
        objectUnderTest.initialize();
        return objectUnderTest;
    }

    @Test
    void initialize_calls_initializeStore() {
        final SourceCoordinator<String> objectUnderTest = new LeaseBasedSourceCoordinator<>(String.class, sourceCoordinationStore, sourceCoordinationConfig, sourceIdentifier, pluginMetrics);
        objectUnderTest.initialize();

        verify(sourceCoordinationStore).initializeStore();
        verify(sourceCoordinationStore).tryCreatePartitionItem(fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS,
                SourcePartitionStatus.UNASSIGNED, 0L, null, false);
    }

    @Test
    void getNextPartition_throws_UninitializedSourceCoordinatorException_when_called_before_initialize() {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = (map) -> List.of(partitionIdentifier);

        final SourceCoordinator<String> objectUnderTest = new LeaseBasedSourceCoordinator<>(String.class, sourceCoordinationStore, sourceCoordinationConfig, sourceIdentifier, pluginMetrics);
        assertThrows(UninitializedSourceCoordinatorException.class, () -> objectUnderTest.getNextPartition(partitionCreationSupplier));
    }

    @Test
    void getNextPartition_calls_supplier_and_creates_partition_with_existing_then_non_existing_item_when_partition_exists_and_is_created_successfully() {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final PartitionIdentifier partitionIdentifierToSkip = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = (map) -> List.of(partitionIdentifierToSkip, partitionIdentifier);

        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.empty()).willReturn( Optional.empty());
        given(globalStateForPartitionCreationItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);
        given(globalStateForPartitionCreationItem.getPartitionOwner()).willReturn(null);
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS)).willReturn(Optional.of(globalStateForPartitionCreationItem));
        doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(globalStateForPartitionCreationItem);
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, partitionIdentifierToSkip.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, partitionIdentifier.getPartitionKey())).willReturn(Optional.empty());
        given(sourceCoordinationStore.tryCreatePartitionItem(fullSourceIdentifierForPartition, partitionIdentifier.getPartitionKey(), SourcePartitionStatus.UNASSIGNED, 0L, null, false)).willReturn(true);

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(partitionCreationSupplier);

        assertThat(result.isEmpty(), equalTo(true));

        verify(globalStateForPartitionCreationItem).setPartitionOwner(anyString());
        verify(globalStateForPartitionCreationItem).setPartitionOwnershipTimeout(any(Instant.class));
        verify(globalStateForPartitionCreationItem).setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);

        verify(partitionCreationSupplierInvocationsCounter).increment();
        verify(noPartitionsAcquiredCounter).increment();
        verify(partitionsCreatedCounter).increment();

        verifyNoInteractions(
                partitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void getNextPartition_calls_supplier_which_returns_existing_partition_does_not_create_the_existing_partition() {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = (map) -> List.of(partitionIdentifier);

        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.empty()).willReturn( Optional.empty());
        given(globalStateForPartitionCreationItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.ASSIGNED);
        given(globalStateForPartitionCreationItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().minusSeconds(120));
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS)).willReturn(Optional.of(globalStateForPartitionCreationItem));
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, partitionIdentifier.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(partitionCreationSupplier);

        assertThat(result.isEmpty(), equalTo(true));

        verify(sourceCoordinationStore, never()).tryCreatePartitionItem(anyString(), anyString(), any(), anyLong(), anyString(), eq(false));

        verify(partitionCreationSupplierInvocationsCounter).increment();
        verify(noPartitionsAcquiredCounter).increment();

        verifyNoInteractions(
                partitionsCreatedCounter,
                partitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void getNextPartition_with_non_existing_item_and_create_attempt_fails_will_do_nothing() throws UnknownHostException {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = (map) -> List.of(partitionIdentifier);

        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.empty()).willReturn( Optional.empty());
        given(globalStateForPartitionCreationItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.ASSIGNED);
        given(globalStateForPartitionCreationItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS)).willReturn(Optional.of(globalStateForPartitionCreationItem));
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, partitionIdentifier.getPartitionKey())).willReturn(Optional.empty());
        given(sourceCoordinationStore.tryCreatePartitionItem(fullSourceIdentifierForPartition, partitionIdentifier.getPartitionKey(), SourcePartitionStatus.UNASSIGNED, 0L, null, false)).willReturn(false);

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition(partitionCreationSupplier);

        assertThat(result.isEmpty(), equalTo(true));

        verify(partitionCreationSupplierInvocationsCounter).increment();
        verify(noPartitionsAcquiredCounter).increment();

        verifyNoInteractions(
                partitionsCreatedCounter,
                partitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);


    }

    @Test
    void getNextPartition_with_no_active_partition_and_unsuccessful_tryAcquireAvailablePartition_returns_empty_Optional() {
        given(globalStateForPartitionCreationItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.ASSIGNED);
        given(globalStateForPartitionCreationItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().plusSeconds(120));
        given(globalStateForPartitionCreationItem.getPartitionOwner()).willReturn(UUID.randomUUID().toString());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS)).willReturn(Optional.of(globalStateForPartitionCreationItem));
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.empty()).willReturn(Optional.empty());


        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition((map) -> Collections.emptyList());

        assertThat(result.isEmpty(), equalTo(true));

        verify(sourceCoordinationStore, never()).tryUpdateSourcePartitionItem(globalStateForPartitionCreationItem);

        verify(noPartitionsAcquiredCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsCreatedCounter,
                partitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void getNextPartition_with_no_active_partition_and_successful_tryAcquireAvailablePartition_returns_expected_SourcePartition() {
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(UUID.randomUUID().toString());
        final String partitionProgressStateValue = UUID.randomUUID().toString();
        given(sourcePartitionStoreItem.getPartitionProgressState()).willReturn("\"" + partitionProgressStateValue + "\"");
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.of(sourcePartitionStoreItem));

        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition((map) -> Collections.emptyList());


        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get().getPartitionKey(), equalTo(sourcePartitionStoreItem.getSourcePartitionKey()));
        assertThat(result.get().getPartitionState().isPresent(), equalTo(true));
        assertThat(result.get().getPartitionState().get(), equalTo(partitionProgressStateValue));

        verify(sourceCoordinationStore, never()).getSourcePartitionItem(anyString(), anyString());
        verify(sourceCoordinationStore, never()).tryUpdateSourcePartitionItem(any(SourcePartitionStoreItem.class));
        verify(sourceCoordinationStore, never()).tryCreatePartitionItem(anyString(), anyString(), any(), anyLong(), anyString(), eq(false));

        verify(partitionsAcquiredCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void getNextPartition_does_not_run_partition_supplier_when_update_to_acquire_throws() {
        given(globalStateForPartitionCreationItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);
        given(globalStateForPartitionCreationItem.getPartitionOwner()).willReturn(null);
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS)).willReturn(Optional.of(globalStateForPartitionCreationItem));
        doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(globalStateForPartitionCreationItem);
        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.empty()).willReturn(Optional.empty());


        final Optional<SourcePartition<String>> result = createObjectUnderTest().getNextPartition((map) -> Collections.emptyList());

        assertThat(result.isEmpty(), equalTo(true));

        verify(noPartitionsAcquiredCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsCreatedCounter,
                partitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void completePartition_with_owned_partition_key_and_no_store_item_throws_PartitionNotFoundException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        assertThrows(PartitionNotFoundException.class, () -> createObjectUnderTest().completePartition(sourcePartition.getPartitionKey(), false));

        verify(partitionNotFoundErrorCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void completePartition_with_owned_partition_key_and_existing_store_item_with_invalid_owner_throws_PartitionNotOwnedException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(UUID.randomUUID().toString());
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(sourcePartition.getPartitionKey());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().completePartition(sourcePartition.getPartitionKey(), false));

        verify(partitionNotOwnedErrorCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void completePartition_with_owned_partition_and_existing_store_item_with_valid_owner_returns_expected_result(final boolean updatedItemSuccessfully) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            createObjectUnderTest().completePartition(sourcePartition.getPartitionKey(), false);

            verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
            verify(sourcePartitionStoreItem).setReOpenAt(null);
            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
            verify(sourcePartitionStoreItem).setPartitionOwner(null);

            verify(partitionsCompletedCounter).increment();
            verifyNoInteractions(completePartitionUpdateErrorCounter);
        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            assertThrows(PartitionUpdateException.class, () -> createObjectUnderTest().completePartition(sourcePartition.getPartitionKey(), false));

            verify(completePartitionUpdateErrorCounter).increment();
            verifyNoInteractions(partitionsCompletedCounter);
        }

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter);
    }

    @Test
    void completePartition_with_fromAcknowledgmentCallback_true_does_not_interact_with_partition_manager() throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));
        doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
        createObjectUnderTest().completePartition(sourcePartition.getPartitionKey(), true);

        verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
        verify(sourcePartitionStoreItem).setReOpenAt(null);
        verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
        verify(sourcePartitionStoreItem).setPartitionOwner(null);
    }

    @Test
    void closePartition_with_owned_partition_key_and_no_store_item_throws_PartitionNotFoundException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        assertThrows(PartitionNotFoundException.class, () -> createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), 1, false));

        verify(partitionNotFoundErrorCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void closePartition_with_owned_partition_key_and_existing_store_item_with_invalid_owner_throws_PartitionNotOwnedException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(UUID.randomUUID().toString());
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(sourcePartition.getPartitionKey());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), 1, false));


        verify(partitionNotOwnedErrorCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @ParameterizedTest
    @MethodSource("getClosedCountArgs")
    void closePartition_with_owned_partition_and_existing_store_item_with_valid_owner_and_below_max_closed_count_returns_expected_result(final boolean updatedItemSuccessfully, final Long closedCount) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourcePartitionStoreItem.getClosedCount()).willReturn(closedCount);

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        final int maxClosedCount = 2;

        if (closedCount >= maxClosedCount) {
            given(sourcePartitionStoreItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.COMPLETED);
        } else {
            given(sourcePartitionStoreItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.CLOSED);
        }

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), maxClosedCount, false);

            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
            verify(sourcePartitionStoreItem).setPartitionOwner(null);

            if (closedCount >= maxClosedCount) {
                verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
                verify(partitionsCompletedCounter).increment();
                verifyNoInteractions(partitionsClosedCounter);
            } else {
                verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.CLOSED);
                verify(sourcePartitionStoreItem).setReOpenAt(any(Instant.class));
                verify(sourcePartitionStoreItem).setClosedCount(closedCount + 1L);
                verify(partitionsClosedCounter).increment();
                verifyNoInteractions(partitionsCompletedCounter);
            }

        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            assertThrows(PartitionUpdateException.class, () -> createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(), Duration.ofMinutes(2), maxClosedCount, false));
            if (closedCount >= maxClosedCount) {
                verify(completePartitionUpdateErrorCounter).increment();
                verifyNoInteractions(closePartitionUpdateErrorCounter);
            } else {
                verify(closePartitionUpdateErrorCounter).increment();
                verifyNoInteractions(completePartitionUpdateErrorCounter);
            }
        }

        verifyNoInteractions(
                partitionNotOwnedErrorCounter,
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                saveStatePartitionUpdateErrorCounter);
    }

    @Test
    void closePartition_with_fromAcknowledgmentCallback_true_does_not_interact_with_partition_manager() throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        final long closedCount = 1;

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourcePartitionStoreItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.COMPLETED);
        given(sourcePartitionStoreItem.getClosedCount()).willReturn(closedCount);

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));
        doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
        createObjectUnderTest().closePartition(sourcePartition.getPartitionKey(),  Duration.ofMinutes(2), 1, true);

        verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
        verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);
        verify(sourcePartitionStoreItem).setPartitionOwner(null);
    }

    @Test
    void savePartitionProgressState_with_owned_partition_key_and_no_store_item_throws_PartitionNotFoundException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        assertThrows(PartitionNotFoundException.class, () -> createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), UUID.randomUUID().toString()));

        verify(partitionNotFoundErrorCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void saveProgressStateForPartition_with_owned_partition_key_and_existing_store_item_with_invalid_owner_throws_PartitionNotOwnedException() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(UUID.randomUUID().toString());
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(sourcePartition.getPartitionKey());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        assertThrows(PartitionNotOwnedException.class, () -> createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), UUID.randomUUID().toString()));

        verify(partitionNotOwnedErrorCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
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

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), newProgressState);

            final ArgumentCaptor<Instant> argumentCaptorForPartitionOwnershipTimeout = ArgumentCaptor.forClass(Instant.class);
            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(argumentCaptorForPartitionOwnershipTimeout.capture());
            final Instant newPartitionOwnershipTimeout = argumentCaptorForPartitionOwnershipTimeout.getValue();
            assertThat(newPartitionOwnershipTimeout.isAfter(beforeSave.plus(DEFAULT_LEASE_TIMEOUT)), equalTo(true));

            verify(sourcePartitionStoreItem).setPartitionProgressState("\"" + newProgressState + "\"");

            verify(saveProgressStateInvocationSuccessCounter).increment();
            verifyNoInteractions(saveStatePartitionUpdateErrorCounter);
        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);
            assertThrows(PartitionUpdateException.class, () -> createObjectUnderTest().saveProgressStateForPartition(sourcePartition.getPartitionKey(), newProgressState));
            verify(saveStatePartitionUpdateErrorCounter).increment();
            verifyNoInteractions(saveProgressStateInvocationSuccessCounter);
        }

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                closePartitionUpdateErrorCounter,
                partitionNotOwnedErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void updatePartitionForAckWait_updates_partition_ownership_and_removes_active_partition_from_partition_manager() throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        final Instant beforeSave = Instant.now();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);

        final Duration ackTimeout = Duration.ofSeconds(10);
        createObjectUnderTest().updatePartitionForAcknowledgmentWait(sourcePartition.getPartitionKey(), ackTimeout);

        final ArgumentCaptor<Instant> argumentCaptorForPartitionOwnershipTimeout = ArgumentCaptor.forClass(Instant.class);
        verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(argumentCaptorForPartitionOwnershipTimeout.capture());
        final Instant newPartitionOwnershipTimeout = argumentCaptorForPartitionOwnershipTimeout.getValue();
        assertThat(newPartitionOwnershipTimeout.isAfter(beforeSave.plus(ackTimeout)), equalTo(true));
    }

    @Test
    void renewPartitionOwnership_updates_partition_ownership() throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        final Instant beforeSave = Instant.now();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(sourcePartitionStoreItem);

        final Duration ackTimeout = Duration.ofSeconds(10);
        createObjectUnderTest().renewPartitionOwnership(sourcePartition.getPartitionKey());

        final ArgumentCaptor<Instant> argumentCaptorForPartitionOwnershipTimeout = ArgumentCaptor.forClass(Instant.class);
        verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(argumentCaptorForPartitionOwnershipTimeout.capture());
        final Instant newPartitionOwnershipTimeout = argumentCaptorForPartitionOwnershipTimeout.getValue();
        assertThat(newPartitionOwnershipTimeout.isAfter(beforeSave.plus(ackTimeout)), equalTo(true));
    }

    @Test
    void giveUpPartitions_with_active_partitionKey_that_does_not_exist_in_the_store_removes_the_active_partition() {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.empty());

        createObjectUnderTest().giveUpPartition(sourcePartition.getPartitionKey());

        verifyNoInteractions(sourcePartitionStoreItem);
        verifyNoMoreInteractions(sourceCoordinationStore);

        verify(partitionsGivenUpCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionNotOwnedErrorCounter,
                partitionNotFoundErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void giveUpPartitions_with_owned_partition_and_existing_store_item_with_valid_owner_returns_expected_result(final boolean updatedItemSuccessfully) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(eq(sourcePartitionStoreItem), eq(null));
            createObjectUnderTest().giveUpPartition(sourcePartition.getPartitionKey());

            verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
            verify(sourcePartitionStoreItem).setPartitionOwner(null);
            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);

        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(eq(sourcePartitionStoreItem), eq(null));
            createObjectUnderTest().giveUpPartition(sourcePartition.getPartitionKey());
        }

        verify(partitionsGivenUpCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionNotOwnedErrorCounter,
                partitionNotFoundErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);

    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void giveUpPartitions_with_override_timestamp_and_existing_store_item_with_valid_owner_returns_expected_result(final boolean updatedItemSuccessfully) throws UnknownHostException {
        final SourcePartition<String> sourcePartition = SourcePartition.builder(String.class)
                .withPartitionKey(UUID.randomUUID().toString())
                .withPartitionState(null)
                .build();

        final Instant now = Instant.now();

        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, sourcePartition.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));

        if (updatedItemSuccessfully) {
            doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(eq(sourcePartitionStoreItem), eq(now));
            createObjectUnderTest().giveUpPartition(sourcePartition.getPartitionKey(), now);

            verify(sourcePartitionStoreItem).setSourcePartitionStatus(SourcePartitionStatus.UNASSIGNED);
            verify(sourcePartitionStoreItem).setPartitionOwner(null);
            verify(sourcePartitionStoreItem).setPartitionOwnershipTimeout(null);

        } else {
            doThrow(PartitionUpdateException.class).when(sourceCoordinationStore).tryUpdateSourcePartitionItem(eq(sourcePartitionStoreItem), eq(now));
            createObjectUnderTest().giveUpPartition(sourcePartition.getPartitionKey(), now);
        }

        verify(partitionsGivenUpCounter).increment();

        verifyNoInteractions(
                partitionCreationSupplierInvocationsCounter,
                partitionsAcquiredCounter,
                partitionsCreatedCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionNotOwnedErrorCounter,
                partitionNotFoundErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);

    }

    @Test
    void getNextPartition_with_supplier_force_will_run_supplier_if_it_has_not_been_run_for_duration() throws NoSuchFieldException, IllegalAccessException {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final PartitionIdentifier partitionIdentifierToSkip = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = (map) -> List.of(partitionIdentifierToSkip, partitionIdentifier);
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(UUID.randomUUID().toString());

        given(sourcePartitionStoreItem.getPartitionProgressState()).willReturn(null);
        given(sourcePartitionStoreItem.getClosedCount()).willReturn(1L);

        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.of(sourcePartitionStoreItem));
        given(globalStateForPartitionCreationItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.UNASSIGNED);
        given(globalStateForPartitionCreationItem.getPartitionOwner()).willReturn(null);
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForGlobalState, GLOBAL_STATE_SOURCE_PARTITION_KEY_FOR_CREATING_PARTITIONS)).willReturn(Optional.of(globalStateForPartitionCreationItem));
        doNothing().when(sourceCoordinationStore).tryUpdateSourcePartitionItem(globalStateForPartitionCreationItem);
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, partitionIdentifierToSkip.getPartitionKey())).willReturn(Optional.of(sourcePartitionStoreItem));
        given(sourceCoordinationStore.getSourcePartitionItem(fullSourceIdentifierForPartition, partitionIdentifier.getPartitionKey())).willReturn(Optional.empty());
        given(sourceCoordinationStore.tryCreatePartitionItem(fullSourceIdentifierForPartition, partitionIdentifier.getPartitionKey(), SourcePartitionStatus.UNASSIGNED, 0L, null, false)).willReturn(true);

        final SourceCoordinator<String> objectUnderTest = createObjectUnderTest();

        final Instant expiredSupplierRunTime = Instant.now().minus(FORCE_SUPPLIER_AFTER_DURATION).minus(Duration.ofMinutes(1));
        ReflectivelySetField.setField(LeaseBasedSourceCoordinator.class, objectUnderTest, "lastSupplierRunTime", expiredSupplierRunTime);

        final Optional<SourcePartition<String>> result = objectUnderTest.getNextPartition(partitionCreationSupplier, true);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get().getPartitionClosedCount(), equalTo(sourcePartitionStoreItem.getClosedCount()));
        assertThat(result.get().getPartitionKey(), equalTo(sourcePartitionStoreItem.getSourcePartitionKey()));
        assertThat(result.get().getPartitionState().isPresent(), equalTo(false));

        verify(globalStateForPartitionCreationItem).setPartitionOwner(anyString());
        verify(globalStateForPartitionCreationItem).setPartitionOwnershipTimeout(any(Instant.class));
        verify(globalStateForPartitionCreationItem).setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);

        verify(partitionCreationSupplierInvocationsCounter).increment();
        verify(partitionsAcquiredCounter).increment();
        verify(partitionsCreatedCounter).increment();

        verifyNoInteractions(
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void getNextPartition_with_supplier_force_will_not_run_supplier_if_it_has_not_reached_force_duration() throws NoSuchFieldException, IllegalAccessException {
        final PartitionIdentifier partitionIdentifier = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final PartitionIdentifier partitionIdentifierToSkip = PartitionIdentifier.builder().withPartitionKey(UUID.randomUUID().toString()).build();
        final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier = (map) -> List.of(partitionIdentifierToSkip, partitionIdentifier);
        given(sourcePartitionStoreItem.getSourcePartitionKey()).willReturn(UUID.randomUUID().toString());

        given(sourcePartitionStoreItem.getPartitionProgressState()).willReturn(null);
        given(sourcePartitionStoreItem.getClosedCount()).willReturn(1L);

        given(sourceCoordinationStore.tryAcquireAvailablePartition(anyString(), anyString(), any())).willReturn(Optional.of(sourcePartitionStoreItem));

        final SourceCoordinator<String> objectUnderTest = createObjectUnderTest();

        final Optional<SourcePartition<String>> result = objectUnderTest.getNextPartition(partitionCreationSupplier, true);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get().getPartitionClosedCount(), equalTo(sourcePartitionStoreItem.getClosedCount()));
        assertThat(result.get().getPartitionKey(), equalTo(sourcePartitionStoreItem.getSourcePartitionKey()));
        assertThat(result.get().getPartitionState().isPresent(), equalTo(false));

        verify(partitionsAcquiredCounter).increment();


        verifyNoInteractions(
                partitionsCreatedCounter,
                partitionCreationSupplierInvocationsCounter,
                noPartitionsAcquiredCounter,
                partitionsCompletedCounter,
                partitionsClosedCounter,
                saveProgressStateInvocationSuccessCounter,
                partitionsGivenUpCounter,
                partitionNotFoundErrorCounter,
                partitionNotOwnedErrorCounter,
                saveStatePartitionUpdateErrorCounter,
                closePartitionUpdateErrorCounter,
                completePartitionUpdateErrorCounter);
    }

    @Test
    void deletePartition_will_call_delete_partition_on_the_coordination_store() throws UnknownHostException {

        final String partitionKey = UUID.randomUUID().toString();
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);
        given(sourcePartitionStoreItem.getPartitionOwner()).willReturn(sourceIdentifierWithPartitionPrefix + ":" + InetAddress.getLocalHost().getHostName());


        when(sourceCoordinationStore.getSourcePartitionItem(anyString(), eq(partitionKey))).thenReturn(Optional.of(sourcePartitionStoreItem));

        doNothing().when(sourceCoordinationStore).tryDeletePartitionItem(sourcePartitionStoreItem);

        final SourceCoordinator<String> objectUnderTest = createObjectUnderTest();

        objectUnderTest.deletePartition(partitionKey);

        verify(sourceCoordinationStore).tryDeletePartitionItem(sourcePartitionStoreItem);
        verify(partitionsDeletedCounter).increment();
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
