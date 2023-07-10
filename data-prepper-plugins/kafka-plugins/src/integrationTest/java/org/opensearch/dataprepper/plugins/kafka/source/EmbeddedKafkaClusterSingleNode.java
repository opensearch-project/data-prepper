/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import kafka.server.KafkaConfig$;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance, 1 Kafka broker, and 1
 * Confluent Schema Registry instance.
 */
public class EmbeddedKafkaClusterSingleNode extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaClusterSingleNode.class);
    private static final int DEFAULT_BROKER_PORT = 0;
    private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
    private static final String AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.NONE.name;
    private static final String KAFKASTORE_OPERATION_TIMEOUT_MS = "60000";
    private static final String KAFKASTORE_DEBUG = "true";
    private static final String KAFKASTORE_INIT_TIMEOUT = "90000";

    private EmbeddedZooKeeperServer zookeeper;
    private EmbeddedKafkaServer broker;
    private RestApp schemaRegistry;
    private final Properties brokerConfig;
    private boolean running;

    public EmbeddedKafkaClusterSingleNode() {
        this(new Properties());
    }

    public EmbeddedKafkaClusterSingleNode(final Properties brokerConfig) {
        this.brokerConfig = new Properties();
        this.brokerConfig.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
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
        log.debug("Starting a Kafka instance on  ...",
                effectiveBrokerConfig.getProperty(KafkaConfig$.MODULE$.ZkConnectDoc()));
        broker = new EmbeddedKafkaServer(effectiveBrokerConfig);
        log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
                broker.brokerList(), broker.zookeeperConnect());

        final Properties schemaRegistryProps = new Properties();

        schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, KAFKASTORE_OPERATION_TIMEOUT_MS);
        schemaRegistryProps.put(SchemaRegistryConfig.DEBUG_CONFIG, KAFKASTORE_DEBUG);
        schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG, KAFKASTORE_INIT_TIMEOUT);
        schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");


        schemaRegistry = new RestApp(0, zookeeperConnect(), KAFKA_SCHEMAS_TOPIC, "none", schemaRegistryProps);
        schemaRegistry.start();
        running = true;
    }

    private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final EmbeddedZooKeeperServer zookeeper) {
        final Properties effectiveConfig = new Properties();
        effectiveConfig.putAll(brokerConfig);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
        effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
        effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
        effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
        effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
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
        log.info("Stopping Confluent");
        try {
            try {
                if (schemaRegistry != null) {
                    schemaRegistry.stop();
                }
            } catch (final Exception fatal) {
                throw new RuntimeException(fatal);
            }
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
        } finally {
            running = false;
        }
        log.info("Confluent Stopped");
    }

    public String bootstrapServers() {
        return broker.brokerList();
    }

    public String zookeeperConnect() {
        return zookeeper.connectString();
    }

    public String schemaRegistryUrl() {
        return schemaRegistry.restConnect;
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
