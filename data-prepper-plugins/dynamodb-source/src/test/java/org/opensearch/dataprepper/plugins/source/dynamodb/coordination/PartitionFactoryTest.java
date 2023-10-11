/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.InitPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;

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

    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;
    private final String bucketName = UUID.randomUUID().toString();

    private final String prefix = UUID.randomUUID().toString();
    private final String s3Key = UUID.randomUUID().toString();
    private final String sequenceNumber = UUID.randomUUID().toString();

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();

    private final long exportTimeMills = 1695021857760L;
    private final Instant exportTime = Instant.ofEpochMilli(exportTimeMills);

    private final Random random = new Random();

    private final int totalRecords = random.nextInt(10000);
    private final int loadedRecords = random.nextInt(10000);


    @Test
    void testCreateExportPartition() {
        String sourceId = sourceIdentifier + "|" + ExportPartition.PARTITION_TYPE;
        String partitionKey = tableArn + "|" + exportTimeMills;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);

        String state = "{\"exportArn\":\"" + exportArn + "\",\"bucket\":\"" + bucketName + "\",\"prefix\":\"" + prefix + "\",\"exportTime\":\"2023-09-20T08:07:17.407353Z\"}";

        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);

        PartitionFactory factory = new PartitionFactory();
        SourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        ExportPartition exportPartition = (ExportPartition) sourcePartition;
        assertThat(exportPartition.getTableArn(), equalTo(tableArn));
        assertThat(exportPartition.getExportTime(), equalTo(exportTime));
        assertThat(exportPartition.getPartitionType(), equalTo(ExportPartition.PARTITION_TYPE));
        assertThat(exportPartition.getPartitionKey(), equalTo(partitionKey));
        assertThat(exportPartition.getProgressState().isPresent(), equalTo(true));
        assertThat(exportPartition.getProgressState().get().getExportArn(), equalTo(exportArn));
        assertThat(exportPartition.getProgressState().get().getBucket(), equalTo(bucketName));
        assertThat(exportPartition.getProgressState().get().getPrefix(), equalTo(prefix));

    }


    @Test
    void testCreateStreamPartition() {
        String sourceId = sourceIdentifier + "|" + StreamPartition.PARTITION_TYPE;
        String partitionKey = streamArn + "|" + shardId;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);

        String state = "{\"startTime\":" + exportTimeMills + ",\"sequenceNumber\":\"" + sequenceNumber + "\",\"waitForExport\":false}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);


        PartitionFactory factory = new PartitionFactory();
        SourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        StreamPartition streamPartition = (StreamPartition) sourcePartition;
        assertThat(streamPartition.getStreamArn(), equalTo(streamArn));
        assertThat(streamPartition.getShardId(), equalTo(shardId));
        assertThat(streamPartition.getPartitionType(), equalTo(StreamPartition.PARTITION_TYPE));
        assertThat(streamPartition.getPartitionKey(), equalTo(partitionKey));

        assertThat(streamPartition.getProgressState().isPresent(), equalTo(true));
        assertThat(streamPartition.getProgressState().get().getSequenceNumber(), equalTo(sequenceNumber));
        assertThat(streamPartition.getProgressState().get().getStartTime(), equalTo(exportTimeMills));
        assertThat(streamPartition.getProgressState().get().shouldWaitForExport(), equalTo(false));

    }

    @Test
    void testCreateDataFilePartition() {
        String sourceId = sourceIdentifier + "|" + DataFilePartition.PARTITION_TYPE;
        String partitionKey = exportArn + "|" + bucketName + "|" + s3Key;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);
        when(sourcePartitionStoreItem.getSourcePartitionKey()).thenReturn(partitionKey);

        String state = "{\"totalRecords\":" + totalRecords + ",\"loadedRecords\":" + loadedRecords + "}";
        when(sourcePartitionStoreItem.getPartitionProgressState()).thenReturn(state);


        PartitionFactory factory = new PartitionFactory();
        SourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        DataFilePartition dataFilePartition = (DataFilePartition) sourcePartition;
        assertThat(dataFilePartition.getExportArn(), equalTo(exportArn));
        assertThat(dataFilePartition.getBucket(), equalTo(bucketName));
        assertThat(dataFilePartition.getKey(), equalTo(s3Key));
        assertThat(dataFilePartition.getPartitionType(), equalTo(DataFilePartition.PARTITION_TYPE));
        assertThat(dataFilePartition.getPartitionKey(), equalTo(partitionKey));

        assertThat(dataFilePartition.getProgressState().isPresent(), equalTo(true));
        assertThat(dataFilePartition.getProgressState().get().getLoaded(), equalTo(loadedRecords));
        assertThat(dataFilePartition.getProgressState().get().getTotal(), equalTo(totalRecords));

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
        SourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        GlobalState globalState = (GlobalState) sourcePartition;
        assertThat(globalState.getPartitionKey(), equalTo(partitionKey));
        assertNull(globalState.getPartitionType());
        assertThat(globalState.getProgressState().isPresent(), equalTo(true));
        assertThat(globalState.getProgressState().get().size(), equalTo(2));
        assertThat(globalState.getProgressState().get().get("totalRecords"), equalTo(totalRecords));
        assertThat(globalState.getProgressState().get().get("loadedRecords"), equalTo(loadedRecords));


    }

    @Test
    void testCreateInitPartition() {
        String sourceId = sourceIdentifier + "|" + InitPartition.PARTITION_TYPE;
        when(sourcePartitionStoreItem.getSourceIdentifier()).thenReturn(sourceId);

        PartitionFactory factory = new PartitionFactory();
        SourcePartition sourcePartition = factory.apply(sourcePartitionStoreItem);
        assertThat(sourcePartition, notNullValue());
        InitPartition exportPartition = (InitPartition) sourcePartition;
        assertThat(exportPartition.getPartitionKey(), equalTo("GLOBAL"));
        assertThat(exportPartition.getPartitionType(), equalTo(InitPartition.PARTITION_TYPE));


    }
}