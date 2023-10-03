/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.SourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ShardConsumerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    @Mock
    private StreamRecordConverter recordConverter;

    @Mock
    private GlobalState tableInfoGlobalState;

    private StreamCheckpointer checkpointer;

    private StreamPartition streamPartition;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String partitionKeyAttrName = "PK";
    private final String sortKeyAttrName = "SK";


    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String streamArn = tableArn + "/stream/2023-09-14T05:46:45.367";

    private final String shardId = "shardId-" + UUID.randomUUID();
    private final String shardIterator = UUID.randomUUID().toString();


    @BeforeEach
    void setup() {

        StreamProgressState state = new StreamProgressState();
        state.setWaitForExport(false);
        state.setStartTime(0);
        streamPartition = new StreamPartition(streamArn, shardId, Optional.of(state));


        // Mock Global Table Info
        lenient().when(coordinator.getPartition(tableArn)).thenReturn(Optional.of(tableInfoGlobalState));
        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName(partitionKeyAttrName)
                .sortKeyAttributeName(sortKeyAttrName)
                .build();
        lenient().when(tableInfoGlobalState.getProgressState()).thenReturn(Optional.of(metadata.toMap()));

        lenient().when(coordinator.createPartition(any(SourcePartition.class))).thenReturn(true);
        lenient().doNothing().when(coordinator).completePartition(any(SourcePartition.class));
        lenient().doNothing().when(coordinator).saveProgressStateForPartition(any(SourcePartition.class));
        lenient().doNothing().when(coordinator).giveUpPartition(any(SourcePartition.class));

        checkpointer = new StreamCheckpointer(coordinator, streamPartition);

        List<Record> records = buildRecords(10);
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(records)
                .nextShardIterator(null)
                .build();
        when(dynamoDbStreamsClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);
    }


    @Test
    void test_run_shardConsumer_correctly() {

        ShardConsumer shardConsumer = ShardConsumer.builder(dynamoDbStreamsClient)
                .shardIterator(shardIterator)
                .checkpointer(checkpointer)
                .recordConverter(recordConverter)
                .startTime(null)
                .waitForExport(false)
                .build();
        shardConsumer.run();

        // Should call GetRecords
        verify(dynamoDbStreamsClient).getRecords(any(GetRecordsRequest.class));

        // Should write to buffer
        verify(recordConverter).writeToBuffer(any(List.class));

        // Should complete the consumer as reach to end of shard
        verify(coordinator).saveProgressStateForPartition(any(StreamPartition.class));
    }

    /**
     * Helper function to generate some data.
     */
    private List<Record> buildRecords(int count) {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, AttributeValue> data = Map.of(
                    partitionKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                    sortKeyAttrName, AttributeValue.builder().s(UUID.randomUUID().toString()).build());

            StreamRecord streamRecord = StreamRecord.builder()
                    .newImage(data)
                    .sequenceNumber(UUID.randomUUID().toString())
                    .approximateCreationDateTime(Instant.now())
                    .build();
            Record record = Record.builder()
                    .dynamodb(streamRecord)
                    .eventName(OperationType.INSERT)
                    .build();
            records.add(record);
        }

        return records;
    }
}