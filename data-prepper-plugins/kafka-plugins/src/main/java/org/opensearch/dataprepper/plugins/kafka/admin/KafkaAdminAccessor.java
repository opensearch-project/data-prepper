package org.opensearch.dataprepper.plugins.kafka.admin;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.plugins.kafka.consumer.TopicEmptinessMetadata;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaClusterAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaAdminAccessor {
    static final Logger LOG = LoggerFactory.getLogger(KafkaAdminAccessor.class);

    private final AdminClient kafkaAdminClient;
    private final TopicEmptinessMetadata topicEmptinessMetadata;
    private final List<String> consumerGroupIds;

    public KafkaAdminAccessor(final KafkaClusterAuthConfig kafkaClusterAuthConfig, final List<String> consumerGroupIds) {
        Properties authProperties = new Properties();
        KafkaSecurityConfigurer.setAuthProperties(authProperties, kafkaClusterAuthConfig, LOG);
        this.kafkaAdminClient = KafkaAdminClient.create(authProperties);
        this.topicEmptinessMetadata = new TopicEmptinessMetadata();
        this.consumerGroupIds = consumerGroupIds;
    }

    @VisibleForTesting
    KafkaAdminAccessor(final AdminClient kafkaAdminClient, final TopicEmptinessMetadata topicEmptinessMetadata, final List<String> consumerGroupIds) {
        this.kafkaAdminClient = kafkaAdminClient;
        this.topicEmptinessMetadata = topicEmptinessMetadata;
        this.consumerGroupIds = consumerGroupIds;
    }

    public synchronized boolean areTopicsEmpty() {
        final long currentThreadId = Thread.currentThread().getId();
        if (Objects.isNull(topicEmptinessMetadata.getTopicEmptyCheckingOwnerThreadId())) {
            topicEmptinessMetadata.setTopicEmptyCheckingOwnerThreadId(currentThreadId);
        }

        if (currentThreadId != topicEmptinessMetadata.getTopicEmptyCheckingOwnerThreadId() ||
                topicEmptinessMetadata.isWithinCheckInterval(System.currentTimeMillis())) {
            return topicEmptinessMetadata.isTopicEmpty();
        }


        final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        for (String consumerGroupId: consumerGroupIds) {
            final ListConsumerGroupOffsetsResult listConsumerGroupOffsets = kafkaAdminClient.listConsumerGroupOffsets(consumerGroupId);
            try {
                committedOffsets.putAll(listConsumerGroupOffsets.partitionsToOffsetAndMetadata().get());
            } catch (final InterruptedException | ExecutionException e) {
                LOG.error("Caught exception getting committed offset data", e);
                return false;
            }
        }

        final Map<TopicPartition, OffsetSpec> listOffsetsRequest = committedOffsets.keySet().stream()
                .collect(Collectors.toMap(topicPartition -> topicPartition, topicPartition -> OffsetSpec.latest()));
        final Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets;
        try {
            endOffsets = kafkaAdminClient.listOffsets(listOffsetsRequest).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            LOG.error("Caught exception getting end offset data", e);
            return false;
        }

        for (TopicPartition topicPartition : committedOffsets.keySet()) {
            final OffsetAndMetadata offsetAndMetadata = committedOffsets.get(topicPartition);

            if (!endOffsets.containsKey(topicPartition)) {
                LOG.warn("No end offset found for topic partition: {}", topicPartition);
                return false;
            }
            final long endOffset = endOffsets.get(topicPartition).offset();

            topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, true);

            // If there is data in the partition
            if (endOffset != 0L) {
                // If there is no committed offset for the partition or the committed offset is behind the end offset
                if (Objects.isNull(offsetAndMetadata) || offsetAndMetadata.offset() < endOffset) {
                    topicEmptinessMetadata.updateTopicEmptinessStatus(topicPartition, false);
                }
            }
        }

        topicEmptinessMetadata.setLastIsEmptyCheckTime(System.currentTimeMillis());
        return topicEmptinessMetadata.isTopicEmpty();
    }
}
