/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbClientWrapper.SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX;
import static org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb.DynamoDbSourceCoordinationStore.SOURCE_STATUS_COMBINATION_KEY_FORMAT;

@ExtendWith(MockitoExtension.class)
class DynamoDbSourceCoordinationStoreIT {
    private static AmazonDynamoDBLocal dynamoDBLocal;

    @Mock
    private DynamoStoreSettings dynamoStoreSettings;
    @Mock
    private DynamoDbClientFactory dynamoDbClientFactory;
    @Mock
    private PluginMetrics pluginMetrics;

    private Random random;
    private DynamoDbTable<DynamoDbSourcePartitionItem> table;
    private String sourceIdentifier;
    private String partitionKey;
    private String ownerId;
    private long itemVersion;

    @BeforeAll
    static void setUpDynamoDb() {
        dynamoDBLocal = DynamoDBEmbedded.create();
    }

    @AfterAll
    static void tearDownDynamoDb() {
        dynamoDBLocal.shutdown();
    }

    @BeforeEach
    void setUp() {
        random = new Random();
        final DynamoDbClient dynamoDbClient = dynamoDBLocal.dynamoDbClient();

        final String tableName = UUID.randomUUID().toString();
        final String region = UUID.randomUUID().toString();
        final String stsRoleArn = UUID.randomUUID().toString();
        final String stsExternalId = UUID.randomUUID().toString();

        when(dynamoStoreSettings.getTableName()).thenReturn(tableName);
        when(dynamoStoreSettings.getRegion()).thenReturn(region);
        when(dynamoStoreSettings.getStsRoleArn()).thenReturn(stsRoleArn);
        when(dynamoStoreSettings.getStsExternalId()).thenReturn(stsExternalId);
        when(dynamoStoreSettings.getProvisionedReadCapacityUnits()).thenReturn(10L);
        when(dynamoStoreSettings.getProvisionedWriteCapacityUnits()).thenReturn(10L);

        when(dynamoDbClientFactory.provideDynamoDbClient(region, stsRoleArn, stsExternalId)).thenReturn(dynamoDbClient);

        final DynamoDbEnhancedClient dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();
        table = dynamoDbEnhancedClient.table(tableName, TableSchema.fromBean(DynamoDbSourcePartitionItem.class));

        sourceIdentifier = UUID.randomUUID().toString();
        partitionKey = UUID.randomUUID().toString();

        ownerId = UUID.randomUUID().toString();
        itemVersion = random.nextInt(100_000) + 1_000;
    }

    private DynamoDbSourceCoordinationStore createObjectUnderTest() {
        final DynamoDbSourceCoordinationStore objectUnderTest = new DynamoDbSourceCoordinationStore(dynamoStoreSettings, dynamoDbClientFactory, pluginMetrics);
        objectUnderTest.initializeStore();
        return objectUnderTest;
    }

    @Test
    void getSourcePartitionItem_returns_empty_optional_when_not_present_in_table() {
        final Optional<SourcePartitionStoreItem> sourcePartitionItem = createObjectUnderTest().getSourcePartitionItem(sourceIdentifier, partitionKey);

        assertThat(sourcePartitionItem, notNullValue());
        assertThat(sourcePartitionItem.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @EnumSource(SourcePartitionStatus.class)
    void getSourcePartitionItem_returns_item_from_table(final SourcePartitionStatus sourcePartitionStatus) {
        final DynamoDbSourceCoordinationStore objectUnderTest = createObjectUnderTest();

        final DynamoDbSourcePartitionItem putItem = putDynamoDbSourcePartitionItem(sourcePartitionStatus);

        final Optional<SourcePartitionStoreItem> optionalItem = objectUnderTest.getSourcePartitionItem(sourceIdentifier, partitionKey);

        assertThat(optionalItem, notNullValue());
        assertThat(optionalItem.isPresent(), equalTo(true));

        final SourcePartitionStoreItem actualItem = optionalItem.get();

        assertThat(actualItem.getSourceIdentifier(), equalTo(sourceIdentifier));
        assertThat(actualItem.getSourcePartitionKey(), equalTo(partitionKey));
        assertThat(actualItem.getPartitionOwner(), equalTo(ownerId));
        assertThat(actualItem.getSourcePartitionStatus(), equalTo(sourcePartitionStatus));
        assertThat(actualItem, instanceOf(DynamoDbSourcePartitionItem.class));

        final DynamoDbSourcePartitionItem actualDynamoItem = (DynamoDbSourcePartitionItem) actualItem;
        assertThat(actualDynamoItem.getVersion(), equalTo(itemVersion));

        assertThat(actualItem, not(sameInstance(putItem)));
    }

    @ParameterizedTest
    @EnumSource(SourcePartitionStatus.class)
    void tryDeletePartitionItem_deletes_item_from_table(final SourcePartitionStatus sourcePartitionStatus) {
        final DynamoDbSourceCoordinationStore objectUnderTest = createObjectUnderTest();

        final DynamoDbSourcePartitionItem putItem = putDynamoDbSourcePartitionItem(sourcePartitionStatus);

        final DynamoDbSourcePartitionItem partitionItem = new DynamoDbSourcePartitionItem();
        partitionItem.setSourceIdentifier(sourceIdentifier);
        partitionItem.setSourcePartitionKey(partitionKey);
        partitionItem.setVersion(itemVersion);

        objectUnderTest.tryDeletePartitionItem(partitionItem);

        final DynamoDbSourcePartitionItem getItemAfterDelete = table.getItem(putItem);
        assertThat(getItemAfterDelete, nullValue());
    }

    @ParameterizedTest
    @EnumSource(SourcePartitionStatus.class)
    void tryDeletePartitionItem_throws_if_version_has_changed(final SourcePartitionStatus sourcePartitionStatus) {
        final DynamoDbSourceCoordinationStore objectUnderTest = createObjectUnderTest();

        final DynamoDbSourcePartitionItem putItem = createUnsavedPartitionItem(sourcePartitionStatus);
        putItem.setVersion(itemVersion + 1);
        table.putItem(putItem);

        final DynamoDbSourcePartitionItem partitionItem = new DynamoDbSourcePartitionItem();
        partitionItem.setSourceIdentifier(sourceIdentifier);
        partitionItem.setSourcePartitionKey(partitionKey);
        partitionItem.setVersion(itemVersion);

        assertThrows(PartitionUpdateException.class, () -> objectUnderTest.tryDeletePartitionItem(partitionItem));

        final DynamoDbSourcePartitionItem getItemAfterDelete = table.getItem(putItem);
        assertThat(getItemAfterDelete, notNullValue());
    }

    @ParameterizedTest
    @EnumSource(SourcePartitionStatus.class)
    void tryCreatePartitionItem_returns_false_if_item_exists_in_table_already(final SourcePartitionStatus sourcePartitionStatus) {
        final DynamoDbSourceCoordinationStore objectUnderTest = createObjectUnderTest();

        putDynamoDbSourcePartitionItem(sourcePartitionStatus);

        assertThat(objectUnderTest.tryCreatePartitionItem(sourceIdentifier,
                        partitionKey, sourcePartitionStatus, 1L, UUID.randomUUID().toString(), false),
                equalTo(false));

        final DynamoDbSourcePartitionItem getItem = table.getItem(GetItemEnhancedRequest.builder()
                .consistentRead(true)
                .key(Key.builder()
                        .partitionValue(sourceIdentifier)
                        .sortValue(partitionKey)
                        .build())
                .build());

        assertThat(getItem, notNullValue());
        assertThat(getItem.getSourceIdentifier(), equalTo(sourceIdentifier));
        assertThat(getItem.getSourcePartitionKey(), equalTo(partitionKey));
        assertThat(getItem.getSourcePartitionStatus(), equalTo(sourcePartitionStatus));
        assertThat(getItem.getSourceStatusCombinationKey(), equalTo(sourceIdentifier + "|" + sourcePartitionStatus));
        assertThat(getItem.getPartitionOwner(), equalTo(ownerId));
        assertThat(getItem.getPartitionOwnershipTimeout(), notNullValue());
        assertThat(getItem.getReOpenAt(), nullValue());
        assertThat(getItem.getClosedCount(), nullValue());
        assertThat(getItem.getExpirationTime(), nullValue());
    }

    @Test
    void tryCreatePartitionItem_creates_an_item() {
        final String partitionProgressState = UUID.randomUUID().toString();
        final Instant before = Instant.now();
        assertThat(createObjectUnderTest().tryCreatePartitionItem(sourceIdentifier,
                        partitionKey, SourcePartitionStatus.UNASSIGNED, 1L, partitionProgressState, false),
                equalTo(true));

        final DynamoDbSourcePartitionItem getItem = table.getItem(GetItemEnhancedRequest.builder()
                .consistentRead(true)
                .key(Key.builder()
                        .partitionValue(sourceIdentifier)
                        .sortValue(partitionKey)
                        .build())
                .build());

        assertThat(getItem, notNullValue());
        assertThat(getItem.getSourceIdentifier(), equalTo(sourceIdentifier));
        assertThat(getItem.getSourcePartitionKey(), equalTo(partitionKey));
        assertThat(getItem.getSourcePartitionStatus(), equalTo(SourcePartitionStatus.UNASSIGNED));
        assertThat(getItem.getSourceStatusCombinationKey(), equalTo(sourceIdentifier + "|" + SourcePartitionStatus.UNASSIGNED));
        assertThat(getItem.getVersion(), equalTo(0L));
        assertThat(getItem.getPartitionOwner(), nullValue());
        assertThat(getItem.getPartitionOwnershipTimeout(), nullValue());
        assertThat(getItem.getPartitionProgressState(), equalTo(partitionProgressState));
        assertThat(getItem.getReOpenAt(), nullValue());
        assertThat(getItem.getClosedCount(), equalTo(1L));
        assertThat(getItem.getPartitionPriority(), notNullValue());
        assertThat(getItem.getPartitionPriority(), greaterThanOrEqualTo(before.toString()));
        assertThat(getItem.getPartitionPriority(), lessThanOrEqualTo(Instant.now().toString()));
        assertThat(getItem.getExpirationTime(), greaterThanOrEqualTo(before.getEpochSecond()));
        assertThat(getItem.getExpirationTime(), lessThanOrEqualTo(Instant.now().getEpochSecond()));
    }

    @Test
    void tryAcquireAvailablePartition_gets_first_unassigned_partition() throws InterruptedException {
        final DynamoDbSourceCoordinationStore objectUnderTest = createObjectUnderTest();
        final String partitionProgressState = UUID.randomUUID().toString();

        final List<String> partitionKeys = IntStream.rangeClosed(1, 3)
                .mapToObj(i -> UUID.randomUUID() + "_" + i)
                .collect(Collectors.toList());

        for (final String partitionKey : partitionKeys) {
            final boolean createSuccess = objectUnderTest.tryCreatePartitionItem(sourceIdentifier,
                    partitionKey, SourcePartitionStatus.UNASSIGNED, 1L, partitionProgressState, false);
            assertThat(createSuccess, equalTo(true));
            Thread.sleep(150);
        }

        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            final String primaryKey = String.format(SOURCE_STATUS_COMBINATION_KEY_FORMAT, sourceIdentifier, SourcePartitionStatus.UNASSIGNED);
            final Stream<DynamoDbSourcePartitionItem> items = querySourceStatusIndex(primaryKey);

            return items.count() == partitionKeys.size();
        });

        final Optional<SourcePartitionStoreItem> maybeAcquired = objectUnderTest.tryAcquireAvailablePartition(sourceIdentifier, ownerId, Duration.ofSeconds(20));

        assertThat(maybeAcquired, notNullValue());
        assertThat(maybeAcquired.isPresent(), equalTo(true));
        final SourcePartitionStoreItem acquiredItem = maybeAcquired.get();

        assertThat(acquiredItem, notNullValue());
        final String unassignedPartitionKey1 = partitionKeys.get(0);

        assertThat(acquiredItem.getSourceIdentifier(), equalTo(sourceIdentifier));
        assertThat(acquiredItem.getSourcePartitionKey(), equalTo(unassignedPartitionKey1));
        assertThat(acquiredItem.getSourcePartitionStatus(), equalTo(SourcePartitionStatus.ASSIGNED));
        assertThat(acquiredItem.getPartitionOwner(), equalTo(ownerId));
    }

    private Stream<DynamoDbSourcePartitionItem> querySourceStatusIndex(final String partitionKey) {
        final DynamoDbIndex<DynamoDbSourcePartitionItem> sourceStatusIndex = table.index(SOURCE_STATUS_COMBINATION_KEY_GLOBAL_SECONDARY_INDEX);
        final QueryEnhancedRequest queryEnhancedRequest = QueryEnhancedRequest.builder()
                .limit(1)
                .queryConditional(QueryConditional.keyEqualTo(Key.builder().partitionValue(partitionKey).build()))
                .build();

        final SdkIterable<Page<DynamoDbSourcePartitionItem>> pages = sourceStatusIndex.query(queryEnhancedRequest);

        return pages.stream()
                .flatMap(page -> page.items().stream());
    }

    private DynamoDbSourcePartitionItem putDynamoDbSourcePartitionItem(final SourcePartitionStatus sourcePartitionStatus) {
        final DynamoDbSourcePartitionItem putItem = createUnsavedPartitionItem(sourcePartitionStatus);
        table.putItem(putItem);
        return putItem;
    }

    private DynamoDbSourcePartitionItem createUnsavedPartitionItem(final SourcePartitionStatus sourcePartitionStatus) {
        final DynamoDbSourcePartitionItem putItem = new DynamoDbSourcePartitionItem();
        putItem.setSourceIdentifier(sourceIdentifier);
        putItem.setSourcePartitionKey(partitionKey);
        putItem.setPartitionOwner(ownerId);
        putItem.setVersion(itemVersion);
        putItem.setSourcePartitionStatus(sourcePartitionStatus);
        putItem.setSourceStatusCombinationKey(sourceIdentifier + "|" + sourcePartitionStatus);
        putItem.setPartitionOwnershipTimeout(Instant.now().plusSeconds(30));
        return putItem;
    }
}