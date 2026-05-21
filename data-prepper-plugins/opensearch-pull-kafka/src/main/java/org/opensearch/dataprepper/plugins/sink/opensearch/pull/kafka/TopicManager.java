/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Named
public class TopicManager {
    private static final Logger LOG = LoggerFactory.getLogger(TopicManager.class);
    private static final long TOPIC_READY_TIMEOUT_MS = 30_000;
    private static final long PARTITION_POLL_INTERVAL_MS = 500;

    private final String bootstrapServers;

    public TopicManager(final KafkaPullEngineConfig config) {
        this.bootstrapServers = String.join(",", config.getBootstrapServers());
    }

    void createTopicWithPartitions(final String topicName, final int partitionCount) {
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            final NewTopic newTopic = new NewTopic(topicName, partitionCount, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            LOG.info("Created Kafka topic '{}' with {} partition(s)", topicName, partitionCount);
            waitForPartitions(adminClient, topicName, partitionCount);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                LOG.info("Kafka topic '{}' already exists, verifying partition count", topicName);
                ensurePartitionCount(topicName, partitionCount);
            } else {
                throw new RuntimeException("Failed to create Kafka topic: " + topicName, e);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while creating Kafka topic: " + topicName, e);
        }
    }

    private void ensurePartitionCount(final String topicName, final int requiredPartitions) {
        final Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (final AdminClient adminClient = AdminClient.create(adminProps)) {
            final Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(Collections.singleton(topicName))
                    .allTopicNames()
                    .get(TOPIC_READY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            final TopicDescription description = descriptions.get(topicName);
            final int currentPartitions = description.partitions().size();

            if (currentPartitions < requiredPartitions) {
                LOG.info("Topic '{}' has {} partition(s) but {} required, increasing partition count",
                        topicName, currentPartitions, requiredPartitions);
                adminClient.createPartitions(
                        Collections.singletonMap(topicName, NewPartitions.increaseTo(requiredPartitions))
                ).all().get();
                LOG.info("Increased partition count for topic '{}' to {}", topicName, requiredPartitions);
                waitForPartitions(adminClient, topicName, requiredPartitions);
            } else {
                LOG.info("Topic '{}' already has {} partition(s), required {}", topicName, currentPartitions, requiredPartitions);
            }
        } catch (final ExecutionException | TimeoutException e) {
            throw new RuntimeException("Failed to verify/update partition count for topic: " + topicName, e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while verifying partition count for topic: " + topicName, e);
        }
    }

    private void waitForPartitions(final AdminClient adminClient, final String topicName, final int expectedPartitions) {
        final long deadline = System.currentTimeMillis() + TOPIC_READY_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                final Map<String, TopicDescription> descriptions = adminClient
                        .describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(TOPIC_READY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                final TopicDescription description = descriptions.get(topicName);
                if (description != null && description.partitions().size() >= expectedPartitions) {
                    LOG.info("All {} partition(s) ready for topic '{}'", expectedPartitions, topicName);
                    return;
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for topic partitions", e);
            } catch (final ExecutionException | TimeoutException e) {
                LOG.debug("Waiting for topic '{}' partitions to become available", topicName);
            }
            try {
                Thread.sleep(PARTITION_POLL_INTERVAL_MS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for topic partitions", e);
            }
        }
        LOG.warn("Timed out waiting for all {} partition(s) on topic '{}' to become available", expectedPartitions, topicName);
    }
}
