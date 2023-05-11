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
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbClientWrapper.ITEM_DOES_NOT_EXIST_EXPRESSION;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbClientWrapper.ITEM_EXISTS_AND_HAS_LATEST_VERSION;

@ExtendWith(MockitoExtension.class)
public class DynamoDbClientWrapperTest {

    @Mock
    private DynamoDbEnhancedClient dynamoDbEnhancedClient;

    private String region;
    private String stsRoleArn;

    @BeforeEach
    void setup() {
        region = "us-east-1";
        stsRoleArn = "arn:aws:iam::123456789012:role/source-coordination-ddb-role";
    }

    private DynamoDbClientWrapper createObjectUnderTest() {
        try (final MockedStatic<DynamoDbClientFactory> dynamoDbClientFactoryMockedStatic = mockStatic(DynamoDbClientFactory.class)) {
              dynamoDbClientFactoryMockedStatic.when(() -> DynamoDbClientFactory.provideDynamoDbEnhancedClient(region, stsRoleArn)).thenReturn(dynamoDbEnhancedClient);
            return DynamoDbClientWrapper.create(region, stsRoleArn);
        }
    }

    @Test
    void tryCreateTableWithNonExistingTable_creates_table() {

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        final String tableName = UUID.randomUUID().toString();
        final ProvisionedThroughput provisionedThroughput = mock(ProvisionedThroughput.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(dynamoDbEnhancedClient.table(eq(tableName), any(BeanTableSchema.class))).willReturn(table);

        doNothing().when(table).createTable(any(CreateTableEnhancedRequest.class));

        try (MockedStatic<DynamoDbWaiter> dynamoDbWaiterMockedStatic = mockStatic(DynamoDbWaiter.class)) {
            final DynamoDbWaiter dynamoDbWaiter = mock(DynamoDbWaiter.class);
            dynamoDbWaiterMockedStatic.when(DynamoDbWaiter::create).thenReturn(dynamoDbWaiter);
            final WaiterResponse waiterResponse = mock(WaiterResponse.class);
            final ResponseOrException<DescribeTableResponse> response = mock(ResponseOrException.class);
            final DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
            given(response.response()).willReturn(Optional.of(describeTableResponse));
            given(dynamoDbWaiter.waitUntilTableExists(any(DescribeTableRequest.class))).willReturn(waiterResponse);

            given(waiterResponse.matched()).willReturn(response);
            final TableDescription tableDescription = mock(TableDescription.class);
            given(describeTableResponse.table()).willReturn(tableDescription);
            given(tableDescription.tableName()).willReturn(tableName);

            objectUnderTest.tryCreateTable(tableName, provisionedThroughput);
        }
    }

    @Test
    void tryCreateTable_does_not_create_table_when_createTable_throws_ResourceInUseException() {
        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        final String tableName = UUID.randomUUID().toString();
        final ProvisionedThroughput provisionedThroughput = mock(ProvisionedThroughput.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(dynamoDbEnhancedClient.table(eq(tableName), any(BeanTableSchema.class))).willReturn(table);

        doThrow(ResourceInUseException.class).when(table).createTable(any(CreateTableEnhancedRequest.class));

        try (MockedStatic<DynamoDbWaiter> dynamoDbWaiterMockedStatic = mockStatic(DynamoDbWaiter.class)) {
            final DynamoDbWaiter dynamoDbWaiter = mock(DynamoDbWaiter.class);
            dynamoDbWaiterMockedStatic.when(DynamoDbWaiter::create).thenReturn(dynamoDbWaiter);
            final WaiterResponse waiterResponse = mock(WaiterResponse.class);
            final ResponseOrException<DescribeTableResponse> response = mock(ResponseOrException.class);
            final DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
            given(response.response()).willReturn(Optional.of(describeTableResponse));
            given(dynamoDbWaiter.waitUntilTableExists(any(DescribeTableRequest.class))).willReturn(waiterResponse);

            given(waiterResponse.matched()).willReturn(response);
            final TableDescription tableDescription = mock(TableDescription.class);
            given(describeTableResponse.table()).willReturn(tableDescription);
            given(tableDescription.tableName()).willReturn(tableName);

            objectUnderTest.tryCreateTable(tableName, provisionedThroughput);
        }
    }

    @Test
    void tryCreateTableThrows_runtime_exception_when_waiter_returns_empty_describe_response() {
        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        final String tableName = UUID.randomUUID().toString();
        final ProvisionedThroughput provisionedThroughput = mock(ProvisionedThroughput.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(dynamoDbEnhancedClient.table(eq(tableName), any(BeanTableSchema.class))).willReturn(table);

        doThrow(ResourceInUseException.class).when(table).createTable(any(CreateTableEnhancedRequest.class));

        try (MockedStatic<DynamoDbWaiter> dynamoDbWaiterMockedStatic = mockStatic(DynamoDbWaiter.class)) {
            final DynamoDbWaiter dynamoDbWaiter = mock(DynamoDbWaiter.class);
            dynamoDbWaiterMockedStatic.when(DynamoDbWaiter::create).thenReturn(dynamoDbWaiter);
            final WaiterResponse waiterResponse = mock(WaiterResponse.class);
            final ResponseOrException<DescribeTableResponse> response = mock(ResponseOrException.class);
            given(response.response()).willReturn(Optional.empty());
            given(dynamoDbWaiter.waitUntilTableExists(any(DescribeTableRequest.class))).willReturn(waiterResponse);

            given(waiterResponse.matched()).willReturn(response);

            assertThrows(RuntimeException.class, () -> objectUnderTest.tryCreateTable(tableName, provisionedThroughput));
        }
    }

    @Test
    void tryCreatePartitionItem_creates_expected_partition_item() throws NoSuchFieldException, IllegalAccessException {
        final ArgumentCaptor<PutItemEnhancedRequest> putItemEnhancedRequestArgumentCaptor = ArgumentCaptor.forClass(PutItemEnhancedRequest.class);
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doNothing().when(table).putItem(putItemEnhancedRequestArgumentCaptor.capture());

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final boolean result = objectUnderTest.tryCreatePartitionItem(dynamoDbSourcePartitionItem);

        assertThat(result, equalTo(true));

        final PutItemEnhancedRequest<DynamoDbSourcePartitionItem> putItemEnhancedRequest = putItemEnhancedRequestArgumentCaptor.getValue();

        assertThat(putItemEnhancedRequest.item(), equalTo(dynamoDbSourcePartitionItem));
        assertThat(putItemEnhancedRequest.conditionExpression().expression(), equalTo(ITEM_DOES_NOT_EXIST_EXPRESSION));
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void tryCreatePartitionItem_catches_exception_from_putItem_and_returns_false(final Class exception) throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(exception).when(table).putItem(any(PutItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final boolean result = objectUnderTest.tryCreatePartitionItem(dynamoDbSourcePartitionItem);

        assertThat(result, equalTo(false));
    }

    @Test
    void tryUpdatePartitionItem_updates_expected_partition_item_correctly() throws NoSuchFieldException, IllegalAccessException {
        final ArgumentCaptor<PutItemEnhancedRequest> putItemEnhancedRequestArgumentCaptor = ArgumentCaptor.forClass(PutItemEnhancedRequest.class);
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);

        final Long version = (long) new Random().nextInt(10);
        given(dynamoDbSourcePartitionItem.getVersion()).willReturn(version).willReturn(version + 1L);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doNothing().when(table).putItem(putItemEnhancedRequestArgumentCaptor.capture());

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        objectUnderTest.tryUpdatePartitionItem(dynamoDbSourcePartitionItem);
        verify(dynamoDbSourcePartitionItem).setVersion(version + 1L);

        final PutItemEnhancedRequest<DynamoDbSourcePartitionItem> putItemEnhancedRequest = putItemEnhancedRequestArgumentCaptor.getValue();

        assertThat(putItemEnhancedRequest.item(), equalTo(dynamoDbSourcePartitionItem));
        assertThat(putItemEnhancedRequest.conditionExpression().expression(), equalTo(ITEM_EXISTS_AND_HAS_LATEST_VERSION));
        assertThat(putItemEnhancedRequest.conditionExpression().expressionValues(), notNullValue());
        assertThat(putItemEnhancedRequest.conditionExpression().expressionValues().containsKey(":v"), equalTo(true));
        assertThat(putItemEnhancedRequest.conditionExpression().expressionValues().get(":v"), notNullValue());
        assertThat(putItemEnhancedRequest.conditionExpression().expressionValues().get(":v").n(), equalTo(version.toString()));

    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void tryAcquirePartitionItem_catches_exception_from_putItem_and_returns_false(final Class exception) throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);
        final Long version = (long) new Random().nextInt(10);
        given(dynamoDbSourcePartitionItem.getVersion()).willReturn(version).willReturn(version + 1L);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(exception).when(table).putItem(any(PutItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final boolean result = objectUnderTest.tryAcquirePartitionItem(dynamoDbSourcePartitionItem);
        verify(dynamoDbSourcePartitionItem).setVersion(version + 1L);
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void tryUpdatePartitionItem_catches_exception_from_putItem_and_throws_PartitionUpdateException(final Class exception) throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);
        final Long version = (long) new Random().nextInt(10);
        given(dynamoDbSourcePartitionItem.getVersion()).willReturn(version).willReturn(version + 1L);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(exception).when(table).putItem(any(PutItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        assertThrows(PartitionUpdateException.class, () -> objectUnderTest.tryUpdatePartitionItem(dynamoDbSourcePartitionItem));
        verify(dynamoDbSourcePartitionItem).setVersion(version + 1L);
    }

    @Test
    void getSourcePartitionItem_returns_expected_item_when_it_exists() throws NoSuchFieldException, IllegalAccessException {
        final ArgumentCaptor<GetItemEnhancedRequest> getItemEnhancedRequestArgumentCaptor = ArgumentCaptor.forClass(GetItemEnhancedRequest.class);

        final SourcePartitionStoreItem sourcePartitionStoreItem = mock(DynamoDbSourcePartitionItem.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(table.getItem(getItemEnhancedRequestArgumentCaptor.capture())).willReturn((DynamoDbSourcePartitionItem) sourcePartitionStoreItem);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final String sourcePartitionKey = UUID.randomUUID().toString();

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getSourcePartitionItem(sourcePartitionKey);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(sourcePartitionStoreItem));

        final GetItemEnhancedRequest getItemEnhancedRequest = getItemEnhancedRequestArgumentCaptor.getValue();

        assertThat(getItemEnhancedRequest.key(), notNullValue());
        assertThat(getItemEnhancedRequest.key().partitionKeyValue().s(), equalTo(sourcePartitionKey));
    }

    @Test
    void getSourcePartitionItem_returns_empty_optional_when_item_does_not_exist() throws NoSuchFieldException, IllegalAccessException {
        final ArgumentCaptor<GetItemEnhancedRequest> getItemEnhancedRequestArgumentCaptor = ArgumentCaptor.forClass(GetItemEnhancedRequest.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(table.getItem(getItemEnhancedRequestArgumentCaptor.capture())).willReturn(null);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final String sourcePartitionKey = UUID.randomUUID().toString();

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getSourcePartitionItem(sourcePartitionKey);

        assertThat(result.isEmpty(), equalTo(true));

        final GetItemEnhancedRequest getItemEnhancedRequest = getItemEnhancedRequestArgumentCaptor.getValue();

        assertThat(getItemEnhancedRequest.key(), notNullValue());
        assertThat(getItemEnhancedRequest.key().partitionKeyValue().s(), equalTo(sourcePartitionKey));
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void getSourcePartitionItem_catches_exception_from_getItem_and_returns_empty_optional(final Class exception) throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(exception).when(table).getItem(any(GetItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final String sourcePartitionKey = UUID.randomUUID().toString();

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getSourcePartitionItem(sourcePartitionKey);

        assertThat(result.isEmpty(), equalTo(true));
    }

    @Test
    void getSourcePartitionItems_returns_expected_PageIterable() throws NoSuchFieldException, IllegalAccessException {
        final ArgumentCaptor<ScanEnhancedRequest> requestArgumentCaptor = ArgumentCaptor.forClass(ScanEnhancedRequest.class);
        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);

        final Expression filterExpression = mock(Expression.class);

        final PageIterable<DynamoDbSourcePartitionItem> dynamoDbSourcePartitionItemPageIterable = mock(PageIterable.class);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        given(table.scan(requestArgumentCaptor.capture())).willReturn(dynamoDbSourcePartitionItemPageIterable);

        final Optional<PageIterable<DynamoDbSourcePartitionItem>> result = objectUnderTest.getSourcePartitionItems(filterExpression);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(dynamoDbSourcePartitionItemPageIterable));

        final ScanEnhancedRequest scanEnhancedRequest = requestArgumentCaptor.getValue();

        assertThat(scanEnhancedRequest.filterExpression(), equalTo(filterExpression));
        assertThat(scanEnhancedRequest.limit(), equalTo(20));
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void getSourcePartitionItems_catches_exception_from_scan_and_returns_empty_optional(final Class exception) throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(exception).when(table).scan(any(ScanEnhancedRequest.class));

        final Expression filterExpression = mock(Expression.class);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final Optional<PageIterable<DynamoDbSourcePartitionItem>> result = objectUnderTest.getSourcePartitionItems(filterExpression);

        assertThat(result.isEmpty(), equalTo(true));
    }

    static Stream<Class> exceptionProvider() {
        return Stream.of(ConditionalCheckFailedException.class, RuntimeException.class, PartitionUpdateException.class);
    }

    private void reflectivelySetField(final DynamoDbClientWrapper dynamoDbClientWrapper, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = DynamoDbClientWrapper.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(dynamoDbClientWrapper, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
