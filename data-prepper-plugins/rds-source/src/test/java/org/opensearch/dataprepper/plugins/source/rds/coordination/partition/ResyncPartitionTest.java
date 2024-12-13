/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResyncPartitionTest {

    @Mock
    SourcePartitionStoreItem  sourcePartitionStoreItem;

    @Mock
    ResyncProgressState resyncProgressState;

    private ResyncPartition resyncPartition;

    @BeforeEach
    void setUp() {
    }

    @Test
    void test_createPartition_from_parameters() {
        final String database = UUID.randomUUID().toString();
        final String table = UUID.randomUUID().toString();
        final long timestamp = 1234567890L;

        resyncPartition = new ResyncPartition(database, table, timestamp, resyncProgressState);

        assertThat(resyncPartition.getPartitionType(), is(ResyncPartition.PARTITION_TYPE));
        assertThat(resyncPartition.getPartitionKey(), is(database + "|" + table + "|" + timestamp));
        assertThat(resyncPartition.getProgressState(), is(Optional.of(resyncProgressState)));
    }

    @Test
    void test_createPartition_from_storeItem() throws JsonProcessingException {
        final String database = UUID.randomUUID().toString();
        final String table = UUID.randomUUID().toString();
        final long timestamp = 1234567890L;
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(database + "|" + table + "|" + timestamp);
        final String foreignKeyName = UUID.randomUUID().toString();
        final String updatedValue = UUID.randomUUID().toString();
        final String primaryKeyName = UUID.randomUUID().toString();
        ObjectMapper mapper = new ObjectMapper();
        final Map<String, Object> progressStateMap = Map.of(
                "foreignKeyName", foreignKeyName,
                "updatedValue", updatedValue,
                "primaryKeys", List.of(primaryKeyName)
        );
        final String progressStateString = mapper.writeValueAsString(progressStateMap);
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(progressStateString);

        resyncPartition = new ResyncPartition(sourcePartitionStoreItem);

        assertThat(resyncPartition.getPartitionType(), is(ResyncPartition.PARTITION_TYPE));
        assertThat(resyncPartition.getPartitionKey(), is(database + "|" + table + "|" + timestamp));

        assertThat(resyncPartition.getProgressState().isPresent(), is(true));
        ResyncProgressState progressState = resyncPartition.getProgressState().get();
        assertThat(progressState.getForeignKeyName(), is(foreignKeyName));
        assertThat(progressState.getUpdatedValue(), is(updatedValue));
        assertThat(progressState.getPrimaryKeys(), is(List.of(primaryKeyName)));
    }

    @Test
    void test_createPartition_from_storeItem_with_invalid_partition_key_then_throws_exception() {
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn("invalid partition key");

        assertThrows(IllegalArgumentException.class, () -> new ResyncPartition(sourcePartitionStoreItem));
    }
}