/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbSourceCoordinationStore.SOURCE_STATUS_COMBINATION_KEY_FORMAT;

@ExtendWith(MockitoExtension.class)
public class DynamoDbSourceCoordinationStoreTest {

    @Mock
    private DynamoStoreSettings dynamoStoreSettings;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DynamoDbClientWrapper dynamoDbClientWrapper;

    @BeforeEach
    void setup() {
        given(dynamoStoreSettings.getRegion()).willReturn(UUID.randomUUID().toString());
        given(dynamoStoreSettings.getStsRoleArn()).willReturn(UUID.randomUUID().toString());
        given(dynamoStoreSettings.getStsExternalId()).willReturn(UUID.randomUUID().toString());
    }

    private DynamoDbSourceCoordinationStore createObjectUnderTest() {
        try (final MockedStatic<DynamoDbClientWrapper> dynamoDbClientWrapperMockedStatic = mockStatic(DynamoDbClientWrapper.class)) {
            dynamoDbClientWrapperMockedStatic.when(() -> DynamoDbClientWrapper.create(dynamoStoreSettings.getRegion(),
                    dynamoStoreSettings.getStsRoleArn(), dynamoStoreSettings.getStsExternalId()))
                .thenReturn(dynamoDbClientWrapper);
            return new DynamoDbSourceCoordinationStore(dynamoStoreSettings, pluginMetrics);
        }
    }

    @Test
    void initializeStore_calls_tryCreateTable() {
        given(dynamoStoreSettings.getProvisionedReadCapacityUnits()).willReturn((long) new Random().nextInt(10));
        given(dynamoStoreSettings.getProvisionedWriteCapacityUnits()).willReturn((long) new Random().nextInt(10));

        final DynamoDbSourceCoordinationStore objectUnderTest = createObjectUnderTest();

        objectUnderTest.initializeStore();

        verify(dynamoDbClientWrapper).initializeTable(eq(dynamoStoreSettings), any(ProvisionedThroughput.class));
    }

    @Test
    void getSourcePartitionItem_calls_dynamoClientWrapper_correctly() {
        final SourcePartitionStoreItem sourcePartitionStoreItem = mock(DynamoDbSourcePartitionItem.class);

        final String sourcePartitionKey = UUID.randomUUID().toString();
        final String sourceIdentifier = UUID.randomUUID().toString();

        given(dynamoDbClientWrapper.getSourcePartitionItem(sourceIdentifier, sourcePartitionKey)).willReturn(Optional.ofNullable(sourcePartitionStoreItem));

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().getSourcePartitionItem(sourceIdentifier, sourcePartitionKey);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(sourcePartitionStoreItem));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void tryCreatePartitionItem_calls_dynamoDbClientWrapper_correctly(final boolean isReadOnlyItem) {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final String sourcePartitionKey = UUID.randomUUID().toString();
        final SourcePartitionStatus sourcePartitionStatus = SourcePartitionStatus.UNASSIGNED;
        final Long closedCount = 0L;
        final String partitionProgressState = UUID.randomUUID().toString();

        final ArgumentCaptor<DynamoDbSourcePartitionItem> argumentCaptor = ArgumentCaptor.forClass(DynamoDbSourcePartitionItem.class);
        given(dynamoDbClientWrapper.tryCreatePartitionItem(argumentCaptor.capture())).willReturn(true);

        final Duration ttl = Duration.ofSeconds(30);
        final Long nowPlusTtl = Instant.now().plus(ttl).getEpochSecond();
        lenient().when(dynamoStoreSettings.getTtl()).thenReturn(ttl);

        final boolean result = createObjectUnderTest().tryCreatePartitionItem(sourceIdentifier, sourcePartitionKey, sourcePartitionStatus, closedCount, partitionProgressState, isReadOnlyItem);

        assertThat(result, equalTo(true));

        final DynamoDbSourcePartitionItem createdItem = argumentCaptor.getValue();
        assertThat(createdItem, notNullValue());
        assertThat(createdItem.getSourceIdentifier(), equalTo(sourceIdentifier));
        assertThat(createdItem.getSourcePartitionKey(), equalTo(sourcePartitionKey));
        assertThat(createdItem.getSourcePartitionStatus(), equalTo(SourcePartitionStatus.UNASSIGNED));
        assertThat(createdItem.getClosedCount(), equalTo(closedCount));
        assertThat(createdItem.getPartitionProgressState(), equalTo(partitionProgressState));
        assertThat(createdItem.getSourceStatusCombinationKey(), equalTo(sourceIdentifier + "|" + sourcePartitionStatus));
        assertThat(createdItem.getPartitionPriority(), notNullValue());

        if (isReadOnlyItem) {
            assertThat(createdItem.getExpirationTime(), nullValue());
        } else {
            assertThat(createdItem.getExpirationTime(), greaterThanOrEqualTo(nowPlusTtl));
        }
    }

    @Test
    void tryUpdateSourcePartitionItem_calls_dynamoClientWrapper_correctly_for_assigned_status() {
        final String sourceIdentifier = UUID.randomUUID().toString();

        final SourcePartitionStoreItem updateItem = mock(DynamoDbSourcePartitionItem.class);
        given(updateItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(updateItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.ASSIGNED);

        given(dynamoStoreSettings.getTtl()).willReturn(null);

        doNothing().when(dynamoDbClientWrapper).tryUpdatePartitionItem((DynamoDbSourcePartitionItem) updateItem);

        final Instant partitionOwnershipTimeout = Instant.now();
        given(updateItem.getPartitionOwnershipTimeout()).willReturn(partitionOwnershipTimeout);
        doNothing().when((DynamoDbSourcePartitionItem)updateItem).setPartitionPriority(partitionOwnershipTimeout.toString());
        createObjectUnderTest().tryUpdateSourcePartitionItem(updateItem);

        verify((DynamoDbSourcePartitionItem) updateItem).setSourceStatusCombinationKey(sourceIdentifier + "|" + SourcePartitionStatus.ASSIGNED);
    }

    @Test
    void tryUpdateSourcePartitionItem_calls_dynamoClientWrapper_correctly_for_closed_status() {
        final String sourceIdentifier = UUID.randomUUID().toString();

        final SourcePartitionStoreItem updateItem = mock(DynamoDbSourcePartitionItem.class);
        given(updateItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(updateItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.CLOSED);

        given(dynamoStoreSettings.getTtl()).willReturn(null);

        doNothing().when(dynamoDbClientWrapper).tryUpdatePartitionItem((DynamoDbSourcePartitionItem) updateItem);
        final Instant reOpenAtTime = Instant.now();
        given(updateItem.getReOpenAt()).willReturn(reOpenAtTime);
        doNothing().when((DynamoDbSourcePartitionItem) updateItem).setPartitionPriority(reOpenAtTime.toString());
        createObjectUnderTest().tryUpdateSourcePartitionItem(updateItem);

        verify((DynamoDbSourcePartitionItem) updateItem).setSourceStatusCombinationKey(sourceIdentifier + "|" + SourcePartitionStatus.CLOSED);
    }

    @Test
    void tryUpdateSourcePartitionItem_calls_dynamoClientWrapper_correctly_for_completed_status() {
        final String sourceIdentifier = UUID.randomUUID().toString();

        final SourcePartitionStoreItem updateItem = mock(DynamoDbSourcePartitionItem.class);
        given(updateItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(updateItem.getSourcePartitionStatus()).willReturn(SourcePartitionStatus.COMPLETED);

        final ArgumentCaptor<Long> argumentCaptor = ArgumentCaptor.forClass(Long.class);
        final Duration ttl = Duration.ofSeconds(30);
        final Long nowPlusTtl = Instant.now().plus(ttl).getEpochSecond();
        given(dynamoStoreSettings.getTtl()).willReturn(ttl);
        doNothing().when((DynamoDbSourcePartitionItem) updateItem).setExpirationTime(argumentCaptor.capture());

        doNothing().when(dynamoDbClientWrapper).tryUpdatePartitionItem((DynamoDbSourcePartitionItem) updateItem);

        createObjectUnderTest().tryUpdateSourcePartitionItem(updateItem);

        final Long expirationTimeResult = argumentCaptor.getValue();
        assertThat(expirationTimeResult, greaterThanOrEqualTo(nowPlusTtl));

        verify((DynamoDbSourcePartitionItem) updateItem).setSourceStatusCombinationKey(sourceIdentifier + "|" + SourcePartitionStatus.COMPLETED);
        verify((DynamoDbSourcePartitionItem) updateItem, never()).setPartitionPriority(anyString());
    }

    @Test
    void getAvailablePartition_with_no_item_acquired_returns_empty_optional() {
        final String ownerId = UUID.randomUUID().toString();
        final String sourceIdentifier = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.ASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.ASSIGNED),
                1))
                .willReturn(Optional.empty());
        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.CLOSED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.CLOSED),
                1))
                .willReturn(Optional.empty());
        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.UNASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.UNASSIGNED),
                5))
                .willReturn(Optional.empty());

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(sourceIdentifier, ownerId, ownershipTimeout);

        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void getAvailablePartition_with_acquired_ASSIGNED_partition_returns_the_partition() {
        final String ownerId = UUID.randomUUID().toString();
        final String sourceIdentifier = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        final DynamoDbSourcePartitionItem acquiredItem = mock(DynamoDbSourcePartitionItem.class);

        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.ASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.ASSIGNED),
                1))
                .willReturn(Optional.of(acquiredItem));

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(sourceIdentifier, ownerId, ownershipTimeout);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(acquiredItem));

        verifyNoMoreInteractions(dynamoDbClientWrapper);
    }

    @Test
    void getAvailablePartition_with_acquired_CLOSED_partition_returns_the_partition() {
        final String ownerId = UUID.randomUUID().toString();
        final String sourceIdentifier = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        final DynamoDbSourcePartitionItem acquiredItem = mock(DynamoDbSourcePartitionItem.class);

        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.ASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.ASSIGNED),
                1))
                .willReturn(Optional.empty());
        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.UNASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.UNASSIGNED),
                5))
                .willReturn(Optional.empty());
        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.CLOSED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.CLOSED),
                1))
                .willReturn(Optional.of(acquiredItem));

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(sourceIdentifier, ownerId, ownershipTimeout);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(acquiredItem));

        verifyNoMoreInteractions(dynamoDbClientWrapper);
    }

    @Test
    void getAvailablePartition_with_acquired_UNASSIGNED_partition_returns_the_partition() {
        final String ownerId = UUID.randomUUID().toString();
        final String sourceIdentifier = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        final DynamoDbSourcePartitionItem acquiredItem = mock(DynamoDbSourcePartitionItem.class);

        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.ASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.ASSIGNED),
                1))
                .willReturn(Optional.empty());
        given(dynamoDbClientWrapper.getAvailablePartition(ownerId, ownershipTimeout,
                SourcePartitionStatus.UNASSIGNED,
                String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.UNASSIGNED),
                5))
                .willReturn(Optional.of(acquiredItem));

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(sourceIdentifier, ownerId, ownershipTimeout);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(acquiredItem));

        verifyNoMoreInteractions(dynamoDbClientWrapper);
    }

    @Test
    void queryAllSourcePartitionItems_success() {
        final String sourceIdentifier = UUID.randomUUID().toString();
        final SourcePartitionStoreItem sourcePartitionStoreItem = mock(DynamoDbSourcePartitionItem.class);
        given(dynamoDbClientWrapper.queryAllPartitions(sourceIdentifier)).willReturn(List.of(sourcePartitionStoreItem));
        List<SourcePartitionStoreItem> sourcePartitionStoreItems = createObjectUnderTest().queryAllSourcePartitionItems(sourceIdentifier);
        assertThat(sourcePartitionStoreItems, is(List.of(sourcePartitionStoreItem)));
    }
}
