/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.source.neptune.coordination;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PartitionFactoryTest {

    @Mock
    private SourcePartitionStoreItem sourcePartitionStoreItem;

    private final String sourceIdentifier = UUID.randomUUID().toString();

    private final String collectionName = UUID.randomUUID().toString();
    private final String databaseName = UUID.randomUUID().toString();
    private final String resumeToken = UUID.randomUUID().toString();
    private final String collection = collectionName + "." + databaseName;
    private final String dataQuery = collection + "|" + UUID.randomUUID();

    private final long exportTimeMills = 1695021857760L;
    private final Instant exportTime = Instant.ofEpochMilli(exportTimeMills);

    private final Random random = new Random();

    private final int partitionSize = random.nextInt(10000);

    private final long totalRecords = random.nextLong();
    private final long loadedRecords = random.nextLong();


    @Test
    void testCreateStreamPartition() {
        String sourceId = sourceIdentifier + "|" + StreamPartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(collection);

        String state = "{\"startTime\":" + exportTimeMills + ",\"commitNum\":1,\"opNum\":1,\"loadedRecords\":" + loadedRecords + ",\"lastUpdateTimestamp\":" + exportTimeMills + ",\"waitForExport\": true}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);


        PartitionFactory factory = new PartitionFactory();
        final StreamPartition streamPartition = (StreamPartition) factory.apply(sourcePartitionStoreItem);
        assertThat(streamPartition, notNullValue());
        assertThat(streamPartition.getPartitionType(), equalTo(StreamPartition.PARTITION_TYPE));
        assertThat(streamPartition.getPartitionKey(), equalTo("neptune"));

        assertThat(streamPartition.getProgressState().isPresent(), equalTo(true));
        assertThat(streamPartition.getProgressState().get().getStartTime(), equalTo(exportTimeMills));
        assertThat(streamPartition.getProgressState().get().getCommitNum(), equalTo(1L));
        assertThat(streamPartition.getProgressState().get().getOpNum(), equalTo(1L));
        assertThat(streamPartition.getProgressState().get().getLoadedRecords(), equalTo(loadedRecords));
        assertThat(streamPartition.getProgressState().get().getLastUpdateTimestamp(), equalTo(exportTimeMills));
        assertThat(streamPartition.getProgressState().get().isWaitForExport(), equalTo(true));
    }


    @Test
    void testCreateDataQueryPartition() {
        String sourceId = sourceIdentifier + "|" + DataQueryPartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(dataQuery);

        String state = "{\"executedQueries\":" + totalRecords + ",\"loadedRecords\":" + loadedRecords + ",\"exportStartTime\":" + exportTimeMills + "}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);


        PartitionFactory factory = new PartitionFactory();
        final DataQueryPartition dataQueryPartition = (DataQueryPartition) factory.apply(sourcePartitionStoreItem);
        assertThat(dataQueryPartition, notNullValue());
        assertThat(dataQueryPartition.getQuery(), equalTo(dataQuery));
        assertThat(dataQueryPartition.getCollection(), equalTo(collection));
        assertThat(dataQueryPartition.getPartitionType(), equalTo(DataQueryPartition.PARTITION_TYPE));
        assertThat(dataQueryPartition.getPartitionKey(), equalTo(dataQuery));

        assertThat(dataQueryPartition.getProgressState().isPresent(), equalTo(true));
        assertThat(dataQueryPartition.getProgressState().get().getExecutedQueries(), equalTo(totalRecords));
        assertThat(dataQueryPartition.getProgressState().get().getLoadedRecords(), equalTo(loadedRecords));
        assertThat(dataQueryPartition.getProgressState().get().getStartTime(), equalTo(exportTimeMills));

    }

    @Test
    void testCreateGlobalState() {

        String sourceId = sourceIdentifier + "|GLOBAL";
        String partitionKey = UUID.randomUUID().toString();
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);

        String state = "{\"totalRecords\":" + totalRecords + ",\"loadedRecords\":" + loadedRecords + "}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);


        PartitionFactory factory = new PartitionFactory();
        GlobalState globalState = (GlobalState) factory.apply(sourcePartitionStoreItem);
        assertThat(globalState, notNullValue());
        assertThat(globalState.getPartitionKey(), equalTo(partitionKey));
        assertNull(globalState.getPartitionType());
        assertThat(globalState.getProgressState().isPresent(), equalTo(true));
        assertThat(globalState.getProgressState().get().size(), equalTo(2));
        assertThat(globalState.getProgressState().get().get("totalRecords"), equalTo(totalRecords));
        assertThat(globalState.getProgressState().get().get("loadedRecords"), equalTo(loadedRecords));


    }

    @Test
    void testCreateLeaderPartition() {
        String sourceId = sourceIdentifier + "|" + LeaderPartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);

        PartitionFactory factory = new PartitionFactory();
        LeaderPartition leaderPartition = (LeaderPartition) factory.apply(sourcePartitionStoreItem);
        assertThat(leaderPartition, notNullValue());
        assertThat(leaderPartition.getPartitionKey(), equalTo("GLOBAL"));
        assertThat(leaderPartition.getPartitionType(), equalTo(LeaderPartition.PARTITION_TYPE));
    }
}
