/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.mongo.coordination;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;

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

    private final int totalRecords = random.nextInt(10000);
    private final int loadedRecords = random.nextInt(10000);


    @Test
    void testCreateExportPartition() {
        String sourceId = sourceIdentifier + "|" + ExportPartition.PARTITION_TYPE;
        String partitionKey = collection + "|" + partitionSize + "|" + exportTimeMills;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);

        String state = "{\"databaseName\":\"" + databaseName + "\",\"collectionName\":\"" + collectionName + "\",\"exportTime\":\"" + exportTime + "\"}";

        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);

        final PartitionFactory factory = new PartitionFactory();
        final ExportPartition exportPartition = (ExportPartition) factory.apply(sourcePartitionStoreItem);
        assertThat(exportPartition, notNullValue());
        assertThat(exportPartition.getPartitionType(), equalTo(ExportPartition.PARTITION_TYPE));
        assertThat(exportPartition.getPartitionKey(), equalTo(partitionKey));
        assertThat(exportPartition.getCollection(), equalTo(collection));
        assertThat(exportPartition.getExportTime(), equalTo(exportTime));
        assertThat(exportPartition.getProgressState().isPresent(), equalTo(true));
        assertThat(exportPartition.getProgressState().get().getCollectionName(), equalTo(collectionName));
        assertThat(exportPartition.getProgressState().get().getDatabaseName(), equalTo(databaseName));
        assertThat(exportPartition.getProgressState().get().getExportTime(), equalTo(exportTime.toString()));

    }


   @Test
    void testCreateStreamPartition() {
        String sourceId = sourceIdentifier + "|" + StreamPartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(collection);

        String state = "{\"startTime\":" + exportTimeMills + ",\"resumeToken\":\"" + resumeToken + "\",\"waitForExport\":false}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);


        PartitionFactory factory = new PartitionFactory();
        final StreamPartition streamPartition = (StreamPartition)factory.apply(sourcePartitionStoreItem);
        assertThat(streamPartition, notNullValue());
        assertThat(streamPartition.getCollection(), equalTo(collection));
        assertThat(streamPartition.getPartitionType(), equalTo(StreamPartition.PARTITION_TYPE));
        assertThat(streamPartition.getPartitionKey(), equalTo(collection));

        assertThat(streamPartition.getProgressState().isPresent(), equalTo(true));
        assertThat(streamPartition.getProgressState().get().getResumeToken(), equalTo(resumeToken));
        assertThat(streamPartition.getProgressState().get().getStartTime(), equalTo(exportTimeMills));
        assertThat(streamPartition.getProgressState().get().shouldWaitForExport(), equalTo(false));

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
