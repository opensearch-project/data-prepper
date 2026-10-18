/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Runs an in-memory, "embedded" Kafka cluster with one ZooKeeper instance and one Kafka broker.
 */
public class EmbeddedKafkaClusterSingleNode extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaClusterSingleNode.class);

    private EmbeddedZooKeeperServer zookeeper;
    private EmbeddedKafkaServer broker;
    private final Properties brokerConfig;

    public EmbeddedKafkaClusterSingleNode() {
        this(new Properties());
    }

    public EmbeddedKafkaClusterSingleNode(final Properties brokerConfig) {
        this.brokerConfig = new Properties();
        this.brokerConfig.putAll(brokerConfig);
    }

    /**
     * Creates and starts the cluster.
     */
    public void start() throws Exception {
        log.debug("Initiating embedded Kafka cluster startup");
        log.debug("Starting a ZooKeeper instance...");
        zookeeper = new EmbeddedZooKeeperServer();
        log.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

        final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
        broker = new EmbeddedKafkaServer(effectiveBrokerConfig);
        log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
                broker.brokerList(), broker.zookeeperConnect());
    }

    private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final EmbeddedZooKeeperServer zookeeper) {
        final Properties effectiveConfig = new Properties();
        effectiveConfig.putAll(brokerConfig);
        return effectiveConfig;
    }

    @Override
    protected void before() throws Exception {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    /**
     * Stops the cluster.
     */
    public void stop() {
        log.info("Stopping embedded Kafka cluster");
        if (broker != null) {
            broker.stop();
        }
        try {
            if (zookeeper != null) {
                zookeeper.stop();
            }
        } catch (final IOException fatal) {
            throw new RuntimeException(fatal);
        }
        log.info("Embedded Kafka cluster stopped");
    }

    public String bootstrapServers() {
        return broker.brokerList();
    }

    public String zookeeperConnect() {
        return zookeeper.connectString();
    }

    public void createTopic(final String topic) {
        createTopic(topic, 1, (short) 1, Collections.emptyMap());
    }

    public void createTopic(final String topic, final int partitions, final short replication) {
        createTopic(topic, partitions, replication, Collections.emptyMap());
    }

    public void createTopic(final String topic,
                            final int partitions,
                            final short replication,
                            final Map<String, String> topicConfig) {
        broker.createTopic(topic, partitions, replication, topicConfig);
    }

}
