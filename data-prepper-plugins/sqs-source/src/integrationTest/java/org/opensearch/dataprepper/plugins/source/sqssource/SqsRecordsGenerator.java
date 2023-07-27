/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SqsRecordsGenerator implements RecordsGenerator {

    private final SqsClient sqsClient;

    public SqsRecordsGenerator(final SqsClient sqsClient){
        this.sqsClient = sqsClient;
    }

    @Override
    public void pushMessages(final List<String> messages, String queueUrl) {
        final List<List<String>> batches = splitIntoBatches(messages, 10);
        batches.forEach(batch -> {
            List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
            batch.forEach(msg -> entries.add(SendMessageBatchRequestEntry.builder()
                    .id(UUID.randomUUID() + "-" + UUID.randomUUID()).messageBody(msg).build()));
            sqsClient.sendMessageBatch(SendMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build());
        });
    }

    private static List<List<String>> splitIntoBatches(List<String> messages, int batchSize) {
        List<List<String>> batches = new ArrayList<>();
        int totalRecords = messages.size();
        int numBatches = (int) Math.ceil((double) totalRecords / batchSize);

        for (int i = 0; i < numBatches; i++) {
            int startIndex = i * batchSize;
            int endIndex = Math.min(startIndex + batchSize, totalRecords);
            List<String> batch = messages.subList(startIndex, endIndex);
            batches.add(batch);
        }
        return batches;
    }
}
