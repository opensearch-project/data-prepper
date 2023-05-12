/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbSourceCoordinationStore.AVAILABLE_PARTITIONS_FILTER_EXPRESSION;

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
    }

    private DynamoDbSourceCoordinationStore createObjectUnderTest() {
        try (final MockedStatic<DynamoDbClientWrapper> dynamoDbClientWrapperMockedStatic = mockStatic(DynamoDbClientWrapper.class)) {
            dynamoDbClientWrapperMockedStatic.when(() -> DynamoDbClientWrapper.create(dynamoStoreSettings.getRegion(), dynamoStoreSettings.getStsRoleArn()))
                            .thenReturn(dynamoDbClientWrapper);
            return new DynamoDbSourceCoordinationStore(dynamoStoreSettings, pluginMetrics);
        }
    }

    @Test
    void initializeStore_calls_tryCreateTable() {
        given(dynamoStoreSettings.getTableName()).willReturn(UUID.randomUUID().toString());
        given(dynamoStoreSettings.getProvisionedReadCapacityUnits()).willReturn((long) new Random().nextInt(10));
        given(dynamoStoreSettings.getProvisionedWriteCapacityUnits()).willReturn((long) new Random().nextInt(10));

        final DynamoDbSourceCoordinationStore objectUnderTest = createObjectUnderTest();

        objectUnderTest.initializeStore();

        verify(dynamoDbClientWrapper).tryCreateTable(eq(dynamoStoreSettings.getTableName()), any(ProvisionedThroughput.class));
    }

    @Test
    void getSourcePartitionItem_calls_dynamoClientWrapper_correctly() {
        final SourcePartitionStoreItem sourcePartitionStoreItem = mock(DynamoDbSourcePartitionItem.class);

        final String partitionKey = UUID.randomUUID().toString();

        given(dynamoDbClientWrapper.getSourcePartitionItem(partitionKey)).willReturn(Optional.ofNullable(sourcePartitionStoreItem));

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().getSourcePartitionItem(partitionKey);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(sourcePartitionStoreItem));
    }

    @Test
    void tryCreatePartitionItem_calls_dynamoDbClientWrapper_correctly() {
        final String partitionKey = UUID.randomUUID().toString();
        final SourcePartitionStatus sourcePartitionStatus = SourcePartitionStatus.UNASSIGNED;
        final Long closedCount = 0L;
        final String partitionProgressState = UUID.randomUUID().toString();

        final ArgumentCaptor<DynamoDbSourcePartitionItem> argumentCaptor = ArgumentCaptor.forClass(DynamoDbSourcePartitionItem.class);
        given(dynamoDbClientWrapper.tryCreatePartitionItem(argumentCaptor.capture())).willReturn(true);

        final boolean result = createObjectUnderTest().tryCreatePartitionItem(partitionKey, sourcePartitionStatus, closedCount, partitionProgressState);

        assertThat(result, equalTo(true));

        final DynamoDbSourcePartitionItem createdItem = argumentCaptor.getValue();
        assertThat(createdItem, notNullValue());
        assertThat(createdItem.getSourcePartitionKey(), equalTo(partitionKey));
        assertThat(createdItem.getSourcePartitionStatus(), equalTo(SourcePartitionStatus.UNASSIGNED));
        assertThat(createdItem.getClosedCount(), equalTo(closedCount));
        assertThat(createdItem.getPartitionProgressState(), equalTo(partitionProgressState));
    }

    @Test
    void tryUpdateSourcePartitionItem_calls_dynamoClientWrapper_correctly() {
        final SourcePartitionStoreItem updateItem = mock(DynamoDbSourcePartitionItem.class);

        doNothing().when(dynamoDbClientWrapper).tryUpdatePartitionItem((DynamoDbSourcePartitionItem) updateItem);

        createObjectUnderTest().tryUpdateSourcePartitionItem(updateItem);
    }

    @Test
    void tryAcquireAvailablePartition_with_empty_page_iterable_returns_empty_optional() {
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        final ArgumentCaptor<Expression> expressionArgumentCaptor = ArgumentCaptor.forClass(Expression.class);

        given(dynamoDbClientWrapper.getSourcePartitionItems(expressionArgumentCaptor.capture())).willReturn(Optional.empty());

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(ownerId, ownershipTimeout);

        assertThat(result.isEmpty(), equalTo(true));

        final Expression expression = expressionArgumentCaptor.getValue();
        assertThat(expression.expression(), equalTo(AVAILABLE_PARTITIONS_FILTER_EXPRESSION));
        assertThat(expression.expressionValues().size(), equalTo(6));
        assertThat(expression.expressionValues().containsKey(":unassigned"), equalTo(true));
        assertThat(expression.expressionValues().containsKey(":closed"), equalTo(true));
        assertThat(expression.expressionValues().containsKey(":assigned"), equalTo(true));
        assertThat(expression.expressionValues().containsKey(":t"), equalTo(true));
        assertThat(expression.expressionValues().containsKey(":ro"), equalTo(true));
        assertThat(expression.expressionValues().containsKey(":null"), equalTo(true));
    }

    @Test
    void tryAcquireAvailablePartition_iterates_until_it_successfully_acquires_a_partition() {
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        final Instant now = Instant.now();

        final PageIterable<DynamoDbSourcePartitionItem> pageIterable = mock(PageIterable.class);

        final List<DynamoDbSourcePartitionItem> itemList = List.of(mock(DynamoDbSourcePartitionItem.class), mock(DynamoDbSourcePartitionItem.class), mock(DynamoDbSourcePartitionItem.class));
        given(pageIterable.items()).willReturn(itemList::iterator);
        given(dynamoDbClientWrapper.getSourcePartitionItems(any(Expression.class))).willReturn(Optional.of(pageIterable));

        doReturn(false).when(dynamoDbClientWrapper).tryAcquirePartitionItem(itemList.get(0));
        doReturn(true).when(dynamoDbClientWrapper).tryAcquirePartitionItem(itemList.get(1));

        final ArgumentCaptor<Instant> argumentCaptor = ArgumentCaptor.forClass(Instant.class);
        doNothing().when(itemList.get(0)).setPartitionOwnershipTimeout(argumentCaptor.capture());

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(ownerId, ownershipTimeout);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), is(itemList.get(1)));

        verifyNoMoreInteractions(dynamoDbClientWrapper);

        verify(itemList.get(0)).setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        verify(itemList.get(0)).setPartitionOwner(ownerId);

        final Instant newOwnershipTimeout = argumentCaptor.getValue();

        assertThat(newOwnershipTimeout, greaterThan(now.plus(ownershipTimeout)));
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void tryAcquireAvailablePartition_returns_empty_optional_when_an_exception_is_thrown_while_iterating(final Class exception) {
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(2);

        final Instant now = Instant.now();

        final PageIterable<DynamoDbSourcePartitionItem> pageIterable = mock(PageIterable.class);

        final List<DynamoDbSourcePartitionItem> itemList = List.of(mock(DynamoDbSourcePartitionItem.class), mock(DynamoDbSourcePartitionItem.class), mock(DynamoDbSourcePartitionItem.class));
        given(pageIterable.items()).willReturn(itemList::iterator);
        given(dynamoDbClientWrapper.getSourcePartitionItems(any(Expression.class))).willReturn(Optional.of(pageIterable));

        doThrow(exception).when(dynamoDbClientWrapper).tryAcquirePartitionItem(itemList.get(0));

        final ArgumentCaptor<Instant> argumentCaptor = ArgumentCaptor.forClass(Instant.class);
        doNothing().when(itemList.get(0)).setPartitionOwnershipTimeout(argumentCaptor.capture());

        final Optional<SourcePartitionStoreItem> result = createObjectUnderTest().tryAcquireAvailablePartition(ownerId, ownershipTimeout);

        assertThat(result.isEmpty(), equalTo(true));

        verifyNoMoreInteractions(dynamoDbClientWrapper);

        verify(itemList.get(0)).setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        verify(itemList.get(0)).setPartitionOwner(ownerId);

        final Instant newOwnershipTimeout = argumentCaptor.getValue();

        assertThat(newOwnershipTimeout, greaterThan(now.plus(ownershipTimeout)));
    }

    static Stream<Class> exceptionProvider() {
        return Stream.of(ProvisionedThroughputExceededException.class, RuntimeException.class);
    }
}
