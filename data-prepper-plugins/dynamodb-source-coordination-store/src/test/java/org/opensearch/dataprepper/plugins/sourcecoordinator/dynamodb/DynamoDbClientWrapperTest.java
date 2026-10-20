/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import software.amazon.awssdk.core.internal.waiters.ResponseOrException;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.BeanTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.DeleteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveDescription;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbClientWrapper.ITEM_DOES_NOT_EXIST_EXPRESSION;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbClientWrapper.ITEM_EXISTS_AND_HAS_LATEST_VERSION;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbClientWrapper.SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbClientWrapper.TTL_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbSourceCoordinationStore.SOURCE_STATUS_COMBINATION_KEY_FORMAT;

@ExtendWith(MockitoExtension.class)
public class DynamoDbClientWrapperTest {
    @Mock
    private DynamoDbEnhancedClient dynamoDbEnhancedClient;

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Mock
    private DynamoStoreSettings dynamoStoreSettings;

    private String region;
    private String stsRoleArn;
    private String sourceIdentifier;

    @BeforeEach
    void setup() {
        region = "us-east-1";
        stsRoleArn = "arn:aws:iam::123456789012:role/source-coordination-ddb-role";
        sourceIdentifier = UUID.randomUUID().toString();
    }

    private DynamoDbClientWrapper createObjectUnderTest() {
        final DynamoDbEnhancedClient.Builder builder = mock(DynamoDbEnhancedClient.Builder.class);
        when(builder.dynamoDbClient(dynamoDbClient)).thenReturn(builder);
        when(builder.build()).thenReturn(dynamoDbEnhancedClient);

        try (final MockedStatic<DynamoDbEnhancedClient> dynamoDbEnhancedClientMockedStatic = mockStatic(DynamoDbEnhancedClient.class)) {
            dynamoDbEnhancedClientMockedStatic.when(DynamoDbEnhancedClient::builder).thenReturn(builder);
            return DynamoDbClientWrapper.create(dynamoDbClient);
        }
    }

    @Test
    void initializeTableWithNonExistingTable_creates_table() {

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        final String tableName = UUID.randomUUID().toString();
        final ProvisionedThroughput provisionedThroughput = mock(ProvisionedThroughput.class);

        given(dynamoStoreSettings.getTableName()).willReturn(tableName);
        given(dynamoStoreSettings.skipTableCreation()).willReturn(false);
        given(dynamoStoreSettings.getTtl()).willReturn(Duration.ofSeconds(30));

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(dynamoDbEnhancedClient.table(eq(tableName), any(BeanTableSchema.class))).willReturn(table);

        doNothing().when(table).createTable(any(CreateTableEnhancedRequest.class));

        final DynamoDbWaiter dynamoDbWaiter = mock(DynamoDbWaiter.class);
        final WaiterResponse waiterResponse = mock(WaiterResponse.class);
        final ResponseOrException<DescribeTableResponse> response = mock(ResponseOrException.class);
        final DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
        given(response.response()).willReturn(Optional.of(describeTableResponse));
        given(dynamoDbWaiter.waitUntilTableExists(any(DescribeTableRequest.class))).willReturn(waiterResponse);

        given(waiterResponse.matched()).willReturn(response);
        final TableDescription tableDescription = mock(TableDescription.class);
        given(describeTableResponse.table()).willReturn(tableDescription);
        given(tableDescription.tableName()).willReturn(tableName);

        given(dynamoDbClient.updateTimeToLive(any(UpdateTimeToLiveRequest.class))).willReturn(mock(UpdateTimeToLiveResponse.class));

        final DynamoDbWaiter.Builder waiterBuilder = mock(DynamoDbWaiter.Builder.class);
        when(waiterBuilder.client(dynamoDbClient)).thenReturn(waiterBuilder);
        when(waiterBuilder.build()).thenReturn(dynamoDbWaiter);
        try (final MockedStatic<DynamoDbWaiter> dynamoDbWaiterMockedStatic = mockStatic(DynamoDbWaiter.class)) {
            dynamoDbWaiterMockedStatic.when(DynamoDbWaiter::builder).thenReturn(waiterBuilder);
            objectUnderTest.initializeTable(dynamoStoreSettings, provisionedThroughput);
        }

        verify(dynamoDbClient, never()).describeTimeToLive(any(DescribeTimeToLiveRequest.class));
    }

    @Test
    void initializeTable_with_null_ttl_and_create_table_when_createTable_throws_ResourceInUseException() {
        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        final String tableName = UUID.randomUUID().toString();
        final ProvisionedThroughput provisionedThroughput = mock(ProvisionedThroughput.class);

        given(dynamoStoreSettings.getTableName()).willReturn(tableName);
        given(dynamoStoreSettings.skipTableCreation()).willReturn(false);
        given(dynamoStoreSettings.getTtl()).willReturn(null);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(dynamoDbEnhancedClient.table(eq(tableName), any(BeanTableSchema.class))).willReturn(table);

        doThrow(ResourceInUseException.class).when(table).createTable(any(CreateTableEnhancedRequest.class));

        final DynamoDbWaiter dynamoDbWaiter = mock(DynamoDbWaiter.class);
        final WaiterResponse waiterResponse = mock(WaiterResponse.class);
        final ResponseOrException<DescribeTableResponse> response = mock(ResponseOrException.class);
        final DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
        given(response.response()).willReturn(Optional.of(describeTableResponse));
        given(dynamoDbWaiter.waitUntilTableExists(any(DescribeTableRequest.class))).willReturn(waiterResponse);
        given(waiterResponse.matched()).willReturn(response);
        final TableDescription tableDescription = mock(TableDescription.class);
        given(describeTableResponse.table()).willReturn(tableDescription);
        given(tableDescription.tableName()).willReturn(tableName);

        final DynamoDbWaiter.Builder waiterBuilder = mock(DynamoDbWaiter.Builder.class);
        when(waiterBuilder.client(dynamoDbClient)).thenReturn(waiterBuilder);
        when(waiterBuilder.build()).thenReturn(dynamoDbWaiter);
        try (final MockedStatic<DynamoDbWaiter> dynamoDbWaiterMockedStatic = mockStatic(DynamoDbWaiter.class)) {
            dynamoDbWaiterMockedStatic.when(DynamoDbWaiter::builder).thenReturn(waiterBuilder);
            objectUnderTest.initializeTable(dynamoStoreSettings, provisionedThroughput);
            verifyNoInteractions(dynamoDbClient);
        }
    }

    @Test
    void initializeTable_Throws_runtime_exception_when_waiter_returns_empty_describe_response_and_skips_table_creation() {
        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        final String tableName = UUID.randomUUID().toString();
        final ProvisionedThroughput provisionedThroughput = mock(ProvisionedThroughput.class);

        given(dynamoStoreSettings.getTableName()).willReturn(tableName);
        given(dynamoStoreSettings.skipTableCreation()).willReturn(true);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(dynamoDbEnhancedClient.table(eq(tableName), any(BeanTableSchema.class))).willReturn(table);

        final DynamoDbWaiter dynamoDbWaiter = mock(DynamoDbWaiter.class);
        final WaiterResponse waiterResponse = mock(WaiterResponse.class);
        final ResponseOrException<DescribeTableResponse> response = mock(ResponseOrException.class);
        given(response.response()).willReturn(Optional.empty());
        given(dynamoDbWaiter.waitUntilTableExists(any(DescribeTableRequest.class))).willReturn(waiterResponse);
        given(waiterResponse.matched()).willReturn(response);

        final DynamoDbWaiter.Builder waiterBuilder = mock(DynamoDbWaiter.Builder.class);
        when(waiterBuilder.client(dynamoDbClient)).thenReturn(waiterBuilder);
        when(waiterBuilder.build()).thenReturn(dynamoDbWaiter);
        try (final MockedStatic<DynamoDbWaiter> dynamoDbWaiterMockedStatic = mockStatic(DynamoDbWaiter.class)) {
            dynamoDbWaiterMockedStatic.when(DynamoDbWaiter::builder).thenReturn(waiterBuilder);

            assertThrows(RuntimeException.class, () -> objectUnderTest.initializeTable(dynamoStoreSettings, provisionedThroughput));
        }

        verify(table, never()).createTable(any(CreateTableEnhancedRequest.class));
    }

    @Test
    void skip_table_creation_true_does_not_attempt_to_create_the_table_or_enable_ttl() {
        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        final String tableName = UUID.randomUUID().toString();
        final ProvisionedThroughput provisionedThroughput = mock(ProvisionedThroughput.class);

        given(dynamoStoreSettings.getTableName()).willReturn(tableName);
        given(dynamoStoreSettings.skipTableCreation()).willReturn(true);
        given(dynamoStoreSettings.getTtl()).willReturn(Duration.ofSeconds(30));

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(dynamoDbEnhancedClient.table(eq(tableName), any(BeanTableSchema.class))).willReturn(table);

        final DynamoDbWaiter dynamoDbWaiter = mock(DynamoDbWaiter.class);

        final WaiterResponse waiterResponse = mock(WaiterResponse.class);
        final ResponseOrException<DescribeTableResponse> response = mock(ResponseOrException.class);
        final DescribeTableResponse describeTableResponse = mock(DescribeTableResponse.class);
        given(response.response()).willReturn(Optional.of(describeTableResponse));
        given(dynamoDbWaiter.waitUntilTableExists(any(DescribeTableRequest.class))).willReturn(waiterResponse);

        given(waiterResponse.matched()).willReturn(response);
        final TableDescription tableDescription = mock(TableDescription.class);
        given(describeTableResponse.table()).willReturn(tableDescription);
        given(tableDescription.tableName()).willReturn(tableName);

        final DescribeTimeToLiveResponse describeTimeToLiveResponse = mock(DescribeTimeToLiveResponse.class);
        final TimeToLiveDescription timeToLiveDescription = mock(TimeToLiveDescription.class);
        given(timeToLiveDescription.attributeName()).willReturn(TTL_ATTRIBUTE_NAME);
        given(timeToLiveDescription.timeToLiveStatus()).willReturn(TimeToLiveStatus.DISABLED);
        given(describeTimeToLiveResponse.timeToLiveDescription()).willReturn(timeToLiveDescription);
        given(dynamoDbClient.describeTimeToLive(any(DescribeTimeToLiveRequest.class))).willReturn(describeTimeToLiveResponse);

        final DynamoDbWaiter.Builder waiterBuilder = mock(DynamoDbWaiter.Builder.class);
        when(waiterBuilder.client(dynamoDbClient)).thenReturn(waiterBuilder);
        when(waiterBuilder.build()).thenReturn(dynamoDbWaiter);
        try (final MockedStatic<DynamoDbWaiter> dynamoDbWaiterMockedStatic = mockStatic(DynamoDbWaiter.class)) {
            dynamoDbWaiterMockedStatic.when(DynamoDbWaiter::builder).thenReturn(waiterBuilder);
            assertThrows(RuntimeException.class, () -> objectUnderTest.initializeTable(dynamoStoreSettings, provisionedThroughput));

            verify(dynamoDbClient, never()).updateTimeToLive(any(UpdateTimeToLiveRequest.class));
            verify(table, never()).createTable(any(CreateTableEnhancedRequest.class));
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

    @Test
    void tryCreatePartitionItem_catches_conditionalCheckFailedException_from_putItem_and_returns_false()
            throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(ConditionalCheckFailedException.class).when(table).putItem(any(PutItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final boolean result = objectUnderTest.tryCreatePartitionItem(dynamoDbSourcePartitionItem);

        assertThat(result, equalTo(false));
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void tryCreatePartitionItem_catches_exception_other_than_conditionalCheckFailedException_from_putItem_and_throws_partitionUpdateException(
            final Class exception) throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(exception).when(table).putItem(any(PutItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        assertThrows(PartitionUpdateException.class,
                () -> objectUnderTest.tryCreatePartitionItem(dynamoDbSourcePartitionItem));
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
    void tryUpdatePartitionItem_rolls_the_version_back_when_the_conditional_check_fails_on_a_deleted_item()
            throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);
        final Long version = (long) new Random().nextInt(10);
        given(dynamoDbSourcePartitionItem.getVersion()).willReturn(version).willReturn(version + 1L);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(ConditionalCheckFailedException.builder().message("deleted").build())
                .when(table).putItem(any(PutItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        assertThrows(PartitionUpdateException.class, () -> objectUnderTest.tryUpdatePartitionItem(dynamoDbSourcePartitionItem));

        verify(dynamoDbSourcePartitionItem).setVersion(version);
        verify(table, never()).getItem(any(GetItemEnhancedRequest.class));
    }

    @Test
    void tryUpdatePartitionItem_leaves_a_writable_version_when_the_table_shows_the_write_landed()
            throws NoSuchFieldException, IllegalAccessException {
        final Long version = (long) new Random().nextInt(10);
        final String owner = UUID.randomUUID().toString();
        final Instant ownershipTimeout = Instant.now();

        final DynamoDbSourcePartitionItem item =
                resyncAgainstTable(version, version + 1L,
                        owner, ownershipTimeout, SourcePartitionStatus.ASSIGNED,
                        owner, ownershipTimeout, SourcePartitionStatus.ASSIGNED);

        assertThat(item.getVersion(), equalTo(version + 1L));
        assertThat(nextWriteWouldSucceed(item, version + 1L), equalTo(true));
    }

    @Test
    void tryUpdatePartitionItem_leaves_an_unwritable_version_when_another_instance_acquired_at_the_same_version()
            throws NoSuchFieldException, IllegalAccessException {
        final Long version = (long) new Random().nextInt(10);

        final DynamoDbSourcePartitionItem item =
                resyncAgainstTable(version, version + 1L,
                        "this-host", Instant.now(), SourcePartitionStatus.ASSIGNED,
                        "another-host", Instant.now().plusSeconds(30), SourcePartitionStatus.ASSIGNED);

        assertThat(nextWriteWouldSucceed(item, version + 1L), equalTo(false));

        for (long newOwnerWrites = 1; newOwnerWrites <= 5; newOwnerWrites++) {
            assertThat("the version must not match the table after the new owner writes " + newOwnerWrites + " more time(s)",
                    nextWriteWouldSucceed(item, version + 1L + newOwnerWrites), equalTo(false));
        }
    }

    @Test
    void partitionOwnershipTimeout_survives_a_round_trip_through_the_table_schema() {
        final TableSchema<DynamoDbSourcePartitionItem> tableSchema = TableSchema.fromBean(DynamoDbSourcePartitionItem.class);
        final Instant ownershipTimeout = Instant.now().plusNanos(123456789L);

        final DynamoDbSourcePartitionItem item = new DynamoDbSourcePartitionItem();
        item.setSourceIdentifier(sourceIdentifier);
        item.setSourcePartitionKey(UUID.randomUUID().toString());
        item.setPartitionOwner(UUID.randomUUID().toString());
        item.setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        item.setPartitionOwnershipTimeout(ownershipTimeout);

        final DynamoDbSourcePartitionItem roundTripped = tableSchema.mapToItem(tableSchema.itemToMap(item, true));

        assertThat(roundTripped.getPartitionOwnershipTimeout(), equalTo(ownershipTimeout));
        assertThat(roundTripped.getPartitionOwner(), equalTo(item.getPartitionOwner()));
        assertThat(roundTripped.getSourcePartitionStatus(), equalTo(item.getSourcePartitionStatus()));
    }

    @Test
    void tryUpdatePartitionItem_undoes_the_increment_when_the_table_shows_the_write_did_not_land()
            throws NoSuchFieldException, IllegalAccessException {
        final Long version = (long) new Random().nextInt(10);
        final String owner = UUID.randomUUID().toString();
        final Instant ownershipTimeout = Instant.now();

        final DynamoDbSourcePartitionItem item =
                resyncAgainstTable(version, version,
                        owner, ownershipTimeout, SourcePartitionStatus.ASSIGNED,
                        owner, ownershipTimeout, SourcePartitionStatus.ASSIGNED);

        assertThat(item.getVersion(), equalTo(version));
        assertThat(nextWriteWouldSucceed(item, version), equalTo(true));
    }

    @Test
    void tryUpdatePartitionItem_leaves_an_unwritable_version_when_another_writer_has_taken_the_partition()
            throws NoSuchFieldException, IllegalAccessException {
        final Long version = (long) new Random().nextInt(10);
        final Long otherWritersVersion = version + 7L;
        final DynamoDbSourcePartitionItem item =
                resyncAgainstTable(version, otherWritersVersion,
                        "this-host", Instant.now(), SourcePartitionStatus.ASSIGNED,
                        "another-host", Instant.now(), SourcePartitionStatus.ASSIGNED);

        assertThat(nextWriteWouldSucceed(item, otherWritersVersion), equalTo(false));
    }

    @Test
    void tryUpdatePartitionItem_keeps_a_writable_version_when_a_retry_fails_its_condition_against_its_own_landed_write()
            throws NoSuchFieldException, IllegalAccessException {
        final Long version = (long) new Random().nextInt(10);
        final String owner = UUID.randomUUID().toString();
        final Instant ownershipTimeout = Instant.now();

        final DynamoDbSourcePartitionItem item =
                conditionalCheckFailureAgainstTable(version, version + 1L,
                        owner, ownershipTimeout, SourcePartitionStatus.ASSIGNED,
                        owner, ownershipTimeout, SourcePartitionStatus.ASSIGNED);

        assertThat(item.getVersion(), equalTo(version + 1L));
        assertThat(nextWriteWouldSucceed(item, version + 1L), equalTo(true));
    }

    @Test
    void tryUpdatePartitionItem_leaves_an_unwritable_version_when_the_conditional_check_fails_against_another_owner()
            throws NoSuchFieldException, IllegalAccessException {
        final Long version = (long) new Random().nextInt(10);

        final DynamoDbSourcePartitionItem item =
                conditionalCheckFailureAgainstTable(version, version + 1L,
                        "this-host", Instant.now(), SourcePartitionStatus.ASSIGNED,
                        "another-host", Instant.now().plusSeconds(30), SourcePartitionStatus.ASSIGNED);

        for (long newOwnerWrites = 0; newOwnerWrites <= 5; newOwnerWrites++) {
            assertThat("the version must not match the table after the new owner writes " + newOwnerWrites + " more time(s)",
                    nextWriteWouldSucceed(item, version + 1L + newOwnerWrites), equalTo(false));
        }
    }

    @Test
    void tryUpdatePartitionItem_sets_the_version_behind_the_table_when_the_table_is_behind_the_attempted_version()
            throws NoSuchFieldException, IllegalAccessException {
        final Long version = (long) new Random().nextInt(10) + 5L;
        final Long storedVersion = version - 1L;

        final DynamoDbSourcePartitionItem item =
                conditionalCheckFailureAgainstTable(version, storedVersion,
                        "this-host", Instant.now(), SourcePartitionStatus.ASSIGNED,
                        "another-host", Instant.now(), SourcePartitionStatus.ASSIGNED);

        assertThat(item.getVersion(), equalTo(storedVersion - 1L));
    }

    @Test
    void tryUpdatePartitionItem_resyncs_with_a_strongly_consistent_read() throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);
        given(dynamoDbSourcePartitionItem.getVersion()).willReturn(1L);
        given(dynamoDbSourcePartitionItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(dynamoDbSourcePartitionItem.getSourcePartitionKey()).willReturn(UUID.randomUUID().toString());

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(RuntimeException.class).when(table).putItem(any(PutItemEnhancedRequest.class));
        final ArgumentCaptor<GetItemEnhancedRequest> getItemRequestCaptor = ArgumentCaptor.forClass(GetItemEnhancedRequest.class);
        given(table.getItem(getItemRequestCaptor.capture())).willReturn(null);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        assertThrows(PartitionUpdateException.class, () -> objectUnderTest.tryUpdatePartitionItem(dynamoDbSourcePartitionItem));

        assertThat(getItemRequestCaptor.getValue().consistentRead(), equalTo(true));
    }

    private boolean nextWriteWouldSucceed(final DynamoDbSourcePartitionItem item, final Long storedVersion) {
        return Objects.equals(item.getVersion(), storedVersion);
    }

    private DynamoDbSourcePartitionItem resyncAgainstTable(final Long version, final Long storedVersion,
                                                           final String owner, final Instant ownershipTimeout,
                                                           final SourcePartitionStatus status,
                                                           final String storedOwner, final Instant storedTimeout,
                                                           final SourcePartitionStatus storedStatus)
            throws NoSuchFieldException, IllegalAccessException {
        return failUpdateAgainstTable(false, version, storedVersion, owner, ownershipTimeout, status,
                storedOwner, storedTimeout, storedStatus);
    }

    private DynamoDbSourcePartitionItem conditionalCheckFailureAgainstTable(final Long version, final Long storedVersion,
                                                                            final String owner, final Instant ownershipTimeout,
                                                                            final SourcePartitionStatus status,
                                                                            final String storedOwner, final Instant storedTimeout,
                                                                            final SourcePartitionStatus storedStatus)
            throws NoSuchFieldException, IllegalAccessException {
        return failUpdateAgainstTable(true, version, storedVersion, owner, ownershipTimeout, status,
                storedOwner, storedTimeout, storedStatus);
    }

    private DynamoDbSourcePartitionItem failUpdateAgainstTable(final boolean conditionalCheckFailure,
                                                               final Long version, final Long storedVersion,
                                                               final String owner, final Instant ownershipTimeout,
                                                               final SourcePartitionStatus status,
                                                               final String storedOwner, final Instant storedTimeout,
                                                               final SourcePartitionStatus storedStatus)
            throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem item = new DynamoDbSourcePartitionItem();
        item.setSourceIdentifier(sourceIdentifier);
        item.setSourcePartitionKey(UUID.randomUUID().toString());
        item.setVersion(version);
        item.setPartitionOwner(owner);
        item.setPartitionOwnershipTimeout(ownershipTimeout);
        item.setSourcePartitionStatus(status);

        final DynamoDbSourcePartitionItem storedItem = new DynamoDbSourcePartitionItem();
        storedItem.setSourceIdentifier(sourceIdentifier);
        storedItem.setSourcePartitionKey(item.getSourcePartitionKey());
        storedItem.setVersion(storedVersion);
        storedItem.setPartitionOwner(storedOwner);
        storedItem.setPartitionOwnershipTimeout(storedTimeout);
        storedItem.setSourcePartitionStatus(storedStatus);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        if (conditionalCheckFailure) {
            final TableSchema<DynamoDbSourcePartitionItem> tableSchema = TableSchema.fromBean(DynamoDbSourcePartitionItem.class);
            given(table.tableSchema()).willReturn(tableSchema);
            doThrow(ConditionalCheckFailedException.builder().item(tableSchema.itemToMap(storedItem, true)).build())
                    .when(table).putItem(any(PutItemEnhancedRequest.class));
        } else {
            doThrow(RuntimeException.class).when(table).putItem(any(PutItemEnhancedRequest.class));
            given(table.getItem(any(GetItemEnhancedRequest.class))).willReturn(storedItem);
        }

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        assertThrows(PartitionUpdateException.class, () -> objectUnderTest.tryUpdatePartitionItem(item));
        if (conditionalCheckFailure) {
            final ArgumentCaptor<PutItemEnhancedRequest> putItemRequestCaptor = ArgumentCaptor.forClass(PutItemEnhancedRequest.class);
            verify(table).putItem(putItemRequestCaptor.capture());
            assertThat(putItemRequestCaptor.getValue().returnValuesOnConditionCheckFailureAsString(), equalTo("ALL_OLD"));
        }
        return item;
    }

    @Test
    void tryUpdatePartitionItem_undoes_the_increment_when_the_resync_read_also_fails() throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);
        final Long version = (long) new Random().nextInt(10);
        given(dynamoDbSourcePartitionItem.getVersion()).willReturn(version).willReturn(version + 1L);
        given(dynamoDbSourcePartitionItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(dynamoDbSourcePartitionItem.getSourcePartitionKey()).willReturn(UUID.randomUUID().toString());

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(RuntimeException.class).when(table).putItem(any(PutItemEnhancedRequest.class));
        doThrow(RuntimeException.class).when(table).getItem(any(GetItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        assertThrows(PartitionUpdateException.class, () -> objectUnderTest.tryUpdatePartitionItem(dynamoDbSourcePartitionItem));
        verify(table).getItem(any(GetItemEnhancedRequest.class));
        verify(dynamoDbSourcePartitionItem).setVersion(version);
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

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getSourcePartitionItem(sourceIdentifier, sourcePartitionKey);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(sourcePartitionStoreItem));

        final GetItemEnhancedRequest getItemEnhancedRequest = getItemEnhancedRequestArgumentCaptor.getValue();

        assertThat(getItemEnhancedRequest.key(), notNullValue());
        assertThat(getItemEnhancedRequest.key().partitionKeyValue().s(), equalTo(sourceIdentifier));
        assertThat(getItemEnhancedRequest.key().sortKeyValue().isPresent(), equalTo(true));
        assertThat(getItemEnhancedRequest.key().sortKeyValue().get().s(), equalTo(sourcePartitionKey));
    }

    @Test
    void getSourcePartitionItem_returns_empty_optional_when_item_does_not_exist() throws NoSuchFieldException, IllegalAccessException {
        final ArgumentCaptor<GetItemEnhancedRequest> getItemEnhancedRequestArgumentCaptor = ArgumentCaptor.forClass(GetItemEnhancedRequest.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        given(table.getItem(getItemEnhancedRequestArgumentCaptor.capture())).willReturn(null);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final String sourcePartitionKey = UUID.randomUUID().toString();

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getSourcePartitionItem(sourceIdentifier, sourcePartitionKey);

        assertThat(result.isEmpty(), equalTo(true));

        final GetItemEnhancedRequest getItemEnhancedRequest = getItemEnhancedRequestArgumentCaptor.getValue();

        assertThat(getItemEnhancedRequest.key(), notNullValue());
        assertThat(getItemEnhancedRequest.key().partitionKeyValue().s(), equalTo(sourceIdentifier));
        assertThat(getItemEnhancedRequest.key().sortKeyValue().isPresent(), equalTo(true));
        assertThat(getItemEnhancedRequest.key().sortKeyValue().get().s(), equalTo(sourcePartitionKey));
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    void getSourcePartitionItem_catches_exception_from_getItem_and_returns_empty_optional(final Class exception) throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        doThrow(exception).when(table).getItem(any(GetItemEnhancedRequest.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final String sourcePartitionKey = UUID.randomUUID().toString();

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getSourcePartitionItem(sourceIdentifier, sourcePartitionKey);

        assertThat(result.isEmpty(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ASSIGNED", "UNASSIGNED", "CLOSED"})
    void getAvailablePartition_with_no_items_from_query_returns_empty_optional(final String sourcePartitionStatus) throws NoSuchFieldException, IllegalAccessException {
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(1);
        final String sourceStatusCombinationKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, sourcePartitionStatus);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final DynamoDbIndex<DynamoDbSourcePartitionItem> sourceStatusIndex = mock(DynamoDbIndex.class);
        given(table.index(SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX)).willReturn(sourceStatusIndex);

        final ArgumentCaptor<QueryEnhancedRequest> queryRequestArgumentCaptor = ArgumentCaptor.forClass(QueryEnhancedRequest.class);

        final SdkIterable<Page<DynamoDbSourcePartitionItem>> pageSdkIterable = () -> {
            final Page<DynamoDbSourcePartitionItem> emptyPage = mock(Page.class);
            given(emptyPage.items()).willReturn(Collections.emptyList());

            return List.of(emptyPage).iterator();
        };


        given(sourceStatusIndex.query(queryRequestArgumentCaptor.capture())).willReturn(pageSdkIterable);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final int pageLimit = new Random().nextInt(20);

        final Duration ttl = Duration.ofSeconds(new Random().nextInt(5)+10);

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.valueOf(sourcePartitionStatus), sourceStatusCombinationKey, pageLimit, ttl);

        assertThat(result.isEmpty(), equalTo(true));

        final QueryEnhancedRequest queryEnhancedRequest = queryRequestArgumentCaptor.getValue();
        assertThat(queryEnhancedRequest, notNullValue());
        assertThat(queryEnhancedRequest.limit(), equalTo(pageLimit));
        assertThat(queryEnhancedRequest.queryConditional(), notNullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ASSIGNED", "UNASSIGNED", "CLOSED"})
    void getAvailablePartition_will_continue_until_tryAcquirePartition_succeeds(final String sourcePartitionStatus) throws NoSuchFieldException, IllegalAccessException {
        final Instant now = Instant.now();
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(1);
        final String sourceStatusCombinationKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, sourcePartitionStatus);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final DynamoDbIndex<DynamoDbSourcePartitionItem> sourceStatusIndex = mock(DynamoDbIndex.class);
        given(table.index(SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX)).willReturn(sourceStatusIndex);

        final DynamoDbSourcePartitionItem unacquiredItem = mock(DynamoDbSourcePartitionItem.class);
        final DynamoDbSourcePartitionItem acquiredItem = mock(DynamoDbSourcePartitionItem.class);
        if (sourcePartitionStatus.equals(SourcePartitionStatus.ASSIGNED.toString())) {
            given(unacquiredItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
            given(acquiredItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
        } else if (sourcePartitionStatus.equals(SourcePartitionStatus.CLOSED.toString())) {
            given(unacquiredItem.getReOpenAt()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
            given(acquiredItem.getReOpenAt()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
        }

        given(acquiredItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(unacquiredItem.getSourceIdentifier()).willReturn(sourceIdentifier);

        final SdkIterable<Page<DynamoDbSourcePartitionItem>> pageSdkIterable = () -> {
            final Page<DynamoDbSourcePartitionItem> page = mock(Page.class);
            given(page.items()).willReturn(List.of(unacquiredItem, acquiredItem));
            return List.of(page).iterator();
        };

        doThrow(ConditionalCheckFailedException.builder().item(Collections.emptyMap()).build())
                .doNothing().when(table).putItem(any(PutItemEnhancedRequest.class));

        given(sourceStatusIndex.query(any(QueryEnhancedRequest.class))).willReturn(pageSdkIterable);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final Duration ttl = Duration.ofSeconds(new Random().nextInt(5)+10);

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.valueOf(sourcePartitionStatus), sourceStatusCombinationKey, new Random().nextInt(20), ttl);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(acquiredItem));

        final ArgumentCaptor<PutItemEnhancedRequest> putItemRequestCaptor = ArgumentCaptor.forClass(PutItemEnhancedRequest.class);
        verify(table, times(2)).putItem(putItemRequestCaptor.capture());
        assertThat(putItemRequestCaptor.getAllValues().get(0).returnValuesOnConditionCheckFailureAsString(), equalTo("NONE"));
        verify(table, never()).tableSchema();
        verify(table, never()).getItem(any(GetItemEnhancedRequest.class));

        verify(acquiredItem).setPartitionOwner(ownerId);
        verify(acquiredItem).setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        verify(acquiredItem).setSourceStatusCombinationKey(sourceIdentifier + "|" + SourcePartitionStatus.ASSIGNED);

        final ArgumentCaptor<Instant> partitionOwnershipArgumentCaptor = ArgumentCaptor.forClass(Instant.class);

        verify(acquiredItem).setPartitionOwnershipTimeout(partitionOwnershipArgumentCaptor.capture());

        final Instant newPartitionOwnershipTimeout = partitionOwnershipArgumentCaptor.getValue();

        assertThat(newPartitionOwnershipTimeout.isAfter(now.plus(ownershipTimeout)), equalTo(true));

        final ArgumentCaptor<Long> expiryTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        verify(acquiredItem).setExpirationTime(expiryTimeArgumentCaptor.capture());
        assertThat(expiryTimeArgumentCaptor.getValue(), greaterThan(Instant.now().getEpochSecond()));

        verify(acquiredItem).setPartitionPriority(newPartitionOwnershipTimeout.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ASSIGNED", "UNASSIGNED", "CLOSED"})
    void getAvailablePartition_with_multiple_pages_continue_until_tryAcquirePartition_succeeds_on_second_page(final String sourcePartitionStatus) throws NoSuchFieldException, IllegalAccessException {
        final Instant now = Instant.now();
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(1);
        final String sourceStatusCombinationKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, sourcePartitionStatus);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final DynamoDbIndex<DynamoDbSourcePartitionItem> sourceStatusIndex = mock(DynamoDbIndex.class);
        given(table.index(SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX)).willReturn(sourceStatusIndex);

        final DynamoDbSourcePartitionItem unacquiredItem = mock(DynamoDbSourcePartitionItem.class);
        final DynamoDbSourcePartitionItem acquiredItem = mock(DynamoDbSourcePartitionItem.class);
        if (sourcePartitionStatus.equals(SourcePartitionStatus.ASSIGNED.toString())) {
            given(unacquiredItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
            given(acquiredItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
        } else if (sourcePartitionStatus.equals(SourcePartitionStatus.CLOSED.toString())) {
            given(unacquiredItem.getReOpenAt()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
            given(acquiredItem.getReOpenAt()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
        }

        given(acquiredItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(unacquiredItem.getSourceIdentifier()).willReturn(sourceIdentifier);

        final SdkIterable<Page<DynamoDbSourcePartitionItem>> pageSdkIterable = () -> {
            final Page<DynamoDbSourcePartitionItem> page = mock(Page.class);
            final Page<DynamoDbSourcePartitionItem> secondPage = mock(Page.class);
            given(page.items()).willReturn(List.of(unacquiredItem));
            given(secondPage.items()).willReturn(List.of(acquiredItem));
            return List.of(page, secondPage).iterator();
        };

        doThrow(PartitionUpdateException.class).doNothing().when(table).putItem(any(PutItemEnhancedRequest.class));

        given(sourceStatusIndex.query(any(QueryEnhancedRequest.class))).willReturn(pageSdkIterable);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final Duration ttl = Duration.ofSeconds(new Random().nextInt(5)+10);

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.valueOf(sourcePartitionStatus), sourceStatusCombinationKey, new Random().nextInt(20), ttl);

        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get(), equalTo(acquiredItem));

        verify(acquiredItem).setPartitionOwner(ownerId);
        verify(acquiredItem).setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        verify(acquiredItem).setSourceStatusCombinationKey(sourceIdentifier + "|" + SourcePartitionStatus.ASSIGNED);

        final ArgumentCaptor<Instant> partitionOwnershipArgumentCaptor = ArgumentCaptor.forClass(Instant.class);

        verify(acquiredItem).setPartitionOwnershipTimeout(partitionOwnershipArgumentCaptor.capture());

        final Instant newPartitionOwnershipTimeout = partitionOwnershipArgumentCaptor.getValue();

        assertThat(newPartitionOwnershipTimeout.isAfter(now.plus(ownershipTimeout)), equalTo(true));

        verify(acquiredItem).setPartitionPriority(newPartitionOwnershipTimeout.toString());

        final ArgumentCaptor<Long> expiryTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        verify(acquiredItem).setExpirationTime(expiryTimeArgumentCaptor.capture());
        assertThat(expiryTimeArgumentCaptor.getValue(), greaterThan(Instant.now().getEpochSecond()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"ASSIGNED", "UNASSIGNED", "CLOSED"})
    void getAvailablePartition_with_multiple_pages_will_iterate_through_all_items_without_acquiring_any(final String sourcePartitionStatus) throws NoSuchFieldException, IllegalAccessException {
        final Instant now = Instant.now();
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(1);
        final String sourceStatusCombinationKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, sourcePartitionStatus);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final DynamoDbIndex<DynamoDbSourcePartitionItem> sourceStatusIndex = mock(DynamoDbIndex.class);
        given(table.index(SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX)).willReturn(sourceStatusIndex);

        final DynamoDbSourcePartitionItem unacquiredItem = mock(DynamoDbSourcePartitionItem.class);
        final DynamoDbSourcePartitionItem acquiredItem = mock(DynamoDbSourcePartitionItem.class);
        if (sourcePartitionStatus.equals(SourcePartitionStatus.ASSIGNED.toString())) {
            given(unacquiredItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
            given(acquiredItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
        } else if (sourcePartitionStatus.equals(SourcePartitionStatus.CLOSED.toString())) {
            given(unacquiredItem.getReOpenAt()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
            given(acquiredItem.getReOpenAt()).willReturn(Instant.now().minus(2, ChronoUnit.MINUTES));
        }

        given(acquiredItem.getSourceIdentifier()).willReturn(sourceIdentifier);
        given(unacquiredItem.getSourceIdentifier()).willReturn(sourceIdentifier);

        final SdkIterable<Page<DynamoDbSourcePartitionItem>> pageSdkIterable = () -> {
            final Page<DynamoDbSourcePartitionItem> page = mock(Page.class);
            final Page<DynamoDbSourcePartitionItem> secondPage = mock(Page.class);
            given(page.items()).willReturn(List.of(unacquiredItem));
            given(secondPage.items()).willReturn(List.of(acquiredItem));
            return List.of(page, secondPage).iterator();
        };

        doThrow(PartitionUpdateException.class).doThrow(PartitionUpdateException.class).when(table).putItem(any(PutItemEnhancedRequest.class));

        given(sourceStatusIndex.query(any(QueryEnhancedRequest.class))).willReturn(pageSdkIterable);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final Duration ttl = Duration.ofSeconds(new Random().nextInt(5)+10);

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.valueOf(sourcePartitionStatus), sourceStatusCombinationKey, new Random().nextInt(20), ttl);

        assertThat(result.isEmpty(), equalTo(true));

        verify(acquiredItem).setPartitionOwner(ownerId);
        verify(acquiredItem).setSourcePartitionStatus(SourcePartitionStatus.ASSIGNED);
        verify(acquiredItem).setSourceStatusCombinationKey(sourceIdentifier + "|" + SourcePartitionStatus.ASSIGNED);

        final ArgumentCaptor<Instant> partitionOwnershipArgumentCaptor = ArgumentCaptor.forClass(Instant.class);

        verify(acquiredItem).setPartitionOwnershipTimeout(partitionOwnershipArgumentCaptor.capture());

        final Instant newPartitionOwnershipTimeout = partitionOwnershipArgumentCaptor.getValue();

        assertThat(newPartitionOwnershipTimeout.isAfter(now.plus(ownershipTimeout)), equalTo(true));

        verify(acquiredItem).setPartitionPriority(newPartitionOwnershipTimeout.toString());

        final ArgumentCaptor<Long> expiryTimeArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        verify(acquiredItem).setExpirationTime(expiryTimeArgumentCaptor.capture());
        assertThat(expiryTimeArgumentCaptor.getValue(), greaterThan(Instant.now().getEpochSecond()));
    }

    @Test
    void getAvailablePartition_with_assigned_partition_with_unexpired_partitionOwnershipTimeout_returns_empty_optional()
            throws NoSuchFieldException, IllegalAccessException {
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(1);
        final String sourceStatusCombinationKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.ASSIGNED);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final DynamoDbIndex<DynamoDbSourcePartitionItem> sourceStatusIndex = mock(DynamoDbIndex.class);
        given(table.index(SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX)).willReturn(sourceStatusIndex);

        final DynamoDbSourcePartitionItem firstItem = mock(DynamoDbSourcePartitionItem.class);
        final DynamoDbSourcePartitionItem secondItem = mock(DynamoDbSourcePartitionItem.class);
        given(firstItem.getPartitionOwnershipTimeout()).willReturn(Instant.now().plus(2, ChronoUnit.MINUTES));

        final SdkIterable<Page<DynamoDbSourcePartitionItem>> pageSdkIterable = () -> {
            final Page<DynamoDbSourcePartitionItem> page = mock(Page.class);
            given(page.items()).willReturn(List.of(firstItem, secondItem));
            return List.of(page).iterator();
        };

        given(sourceStatusIndex.query(any(QueryEnhancedRequest.class))).willReturn(pageSdkIterable);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.ASSIGNED, sourceStatusCombinationKey, new Random().nextInt(20), Duration.ofSeconds(new Random().nextInt()));

        assertThat(result.isEmpty(), equalTo(true));

        verifyNoMoreInteractions(table);
        verifyNoInteractions(secondItem);

    }

    @Test
    void getAvailablePartition_with_closed_partition_with_unreached_reOpenAt_time_returns_empty_optional()
            throws NoSuchFieldException, IllegalAccessException {
        final String ownerId = UUID.randomUUID().toString();
        final Duration ownershipTimeout = Duration.ofMinutes(1);
        final String sourceStatusCombinationKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.CLOSED);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final DynamoDbIndex<DynamoDbSourcePartitionItem> sourceStatusIndex = mock(DynamoDbIndex.class);
        given(table.index(SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX)).willReturn(sourceStatusIndex);

        final DynamoDbSourcePartitionItem firstItem = mock(DynamoDbSourcePartitionItem.class);
        final DynamoDbSourcePartitionItem secondItem = mock(DynamoDbSourcePartitionItem.class);
        given(firstItem.getReOpenAt()).willReturn(Instant.now().plus(2, ChronoUnit.MINUTES));

        final SdkIterable<Page<DynamoDbSourcePartitionItem>> pageSdkIterable = () -> {
            final Page<DynamoDbSourcePartitionItem> page = mock(Page.class);
            given(page.items()).willReturn(List.of(firstItem, secondItem));
            return List.of(page).iterator();
        };

        given(sourceStatusIndex.query(any(QueryEnhancedRequest.class))).willReturn(pageSdkIterable);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final Optional<SourcePartitionStoreItem> result = objectUnderTest.getAvailablePartition(
                ownerId, ownershipTimeout, SourcePartitionStatus.CLOSED, sourceStatusCombinationKey, new Random().nextInt(20), Duration.ofSeconds(new Random().nextInt()));

        assertThat(result.isEmpty(), equalTo(true));

        verifyNoMoreInteractions(table);
        verifyNoInteractions(secondItem);

    }

    @Test
    void queryAllPartitions_success() throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final DynamoDbSourcePartitionItem firstItem = mock(DynamoDbSourcePartitionItem.class);
        final DynamoDbSourcePartitionItem secondItem = mock(DynamoDbSourcePartitionItem.class);
        final PageIterable<DynamoDbSourcePartitionItem> availableItems =  () -> {
            final Page<DynamoDbSourcePartitionItem> page = mock(Page.class);
            given(page.items()).willReturn(List.of(firstItem, secondItem));
            return List.of(page).iterator();
        };

        given(table.query(any(QueryEnhancedRequest.class))).willReturn(availableItems);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final List<SourcePartitionStoreItem> result = objectUnderTest.queryAllPartitions(UUID.randomUUID().toString());

        assertThat(result, is(List.of(firstItem, secondItem)));
        verifyNoMoreInteractions(table);
        verifyNoInteractions(secondItem);

    }

    @Test
    void queryAllPartitions_empty_results() throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        final PageIterable<DynamoDbSourcePartitionItem> availableItems =  () -> {
            final Page<DynamoDbSourcePartitionItem> page = mock(Page.class);
            given(page.items()).willReturn(Collections.emptyList());
            return List.of(page).iterator();
        };

        given(table.query(any(QueryEnhancedRequest.class))).willReturn(availableItems);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();
        reflectivelySetField(objectUnderTest, "table", table);

        final List<SourcePartitionStoreItem> result = objectUnderTest.queryAllPartitions(UUID.randomUUID().toString());

        assertThat(result, is(empty()));
        verifyNoMoreInteractions(table);
    }

    @Test
    void tryDeletePartition_item_success_calls_delete_on_correct_item() throws NoSuchFieldException, IllegalAccessException {

        final String ddbPartitionKey = UUID.randomUUID().toString();
        final String ddbSortKey = UUID.randomUUID().toString();

        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);
        when(dynamoDbSourcePartitionItem.getSourceIdentifier()).thenReturn(ddbPartitionKey);
        when(dynamoDbSourcePartitionItem.getSourcePartitionKey()).thenReturn(ddbSortKey);

        final Long version = (long) new Random().nextInt(10);
        given(dynamoDbSourcePartitionItem.getVersion()).willReturn(version).willReturn(version + 1L);

        final ArgumentCaptor<DeleteItemEnhancedRequest> deleteItemEnhancedRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteItemEnhancedRequest.class);

        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        when(table.deleteItem(deleteItemEnhancedRequestArgumentCaptor.capture())).thenReturn(mock(DynamoDbSourcePartitionItem.class));

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        objectUnderTest.tryDeletePartitionItem(dynamoDbSourcePartitionItem);

        verify(dynamoDbSourcePartitionItem).setVersion(version + 1L);

        final DeleteItemEnhancedRequest deleteItemEnhancedRequest = deleteItemEnhancedRequestArgumentCaptor.getValue();

        assertThat(deleteItemEnhancedRequest.key(), notNullValue());
        assertThat(deleteItemEnhancedRequest.key().partitionKeyValue(), notNullValue());
        assertThat(deleteItemEnhancedRequest.key().partitionKeyValue().s(), equalTo(ddbPartitionKey));
        assertThat(deleteItemEnhancedRequest.key().sortKeyValue(), notNullValue());
        assertThat(deleteItemEnhancedRequest.key().sortKeyValue().isPresent(), equalTo(true));
        assertThat(deleteItemEnhancedRequest.key().sortKeyValue().get().s(), equalTo(ddbSortKey));
        assertThat(deleteItemEnhancedRequest.conditionExpression().expression(), equalTo(ITEM_EXISTS_AND_HAS_LATEST_VERSION));
        assertThat(deleteItemEnhancedRequest.conditionExpression().expressionValues(), notNullValue());
        assertThat(deleteItemEnhancedRequest.conditionExpression().expressionValues().containsKey(":v"), equalTo(true));
        assertThat(deleteItemEnhancedRequest.conditionExpression().expressionValues().get(":v"), notNullValue());
        assertThat(deleteItemEnhancedRequest.conditionExpression().expressionValues().get(":v").n(), equalTo(version.toString()));
    }

    @Test
    void tryDeletePartition_with_exception_throws_PartitionUpdateException() throws NoSuchFieldException, IllegalAccessException {
        final DynamoDbTable<DynamoDbSourcePartitionItem> table = mock(DynamoDbTable.class);
        when(table.deleteItem(any(DeleteItemEnhancedRequest.class))).thenThrow(RuntimeException.class);

        final DynamoDbClientWrapper objectUnderTest = createObjectUnderTest();

        reflectivelySetField(objectUnderTest, "table", table);

        final DynamoDbSourcePartitionItem dynamoDbSourcePartitionItem = mock(DynamoDbSourcePartitionItem.class);
        when(dynamoDbSourcePartitionItem.getSourceIdentifier()).thenReturn(UUID.randomUUID().toString());
        assertThrows(PartitionUpdateException.class, () -> objectUnderTest.tryDeletePartitionItem(dynamoDbSourcePartitionItem));
    }


    static Stream<Class> exceptionProvider() {
        return Stream.of(RuntimeException.class, PartitionUpdateException.class);
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
