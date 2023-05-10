/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicsConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;


/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
  private final KafkaSourceConfig sourceConfig;
  private Set<MultithreadedConsumer> consumers = new HashSet<>();
  private ExecutorService executorService;
  private static final String KAFKA_WORKER_THREAD_PROCESSING_ERRORS = "kafkaWorkerThreadProcessingErrors";
  private final Counter kafkaWorkerThreadProcessingErrors;
  private final PluginMetrics pluginMetrics;
  private String consumerGroup;
  private MultithreadedConsumer multithreadedConsumer;
  private int totalWorkers;

  @DataPrepperPluginConstructor
  public KafkaSource(final KafkaSourceConfig sourceConfig, final PluginMetrics pluginMetrics) {
    this.sourceConfig = sourceConfig;
    this.pluginMetrics = pluginMetrics;
    this.kafkaWorkerThreadProcessingErrors = pluginMetrics.counter(KAFKA_WORKER_THREAD_PROCESSING_ERRORS);
  }

  @Override
  public void start(Buffer<Record<Object>> buffer) {
    sourceConfig.getTopics().forEach(topic -> {
      totalWorkers = 0;
      Properties consumerProperties = getConsumerProperties(topic);
      try {
        totalWorkers = topic.getTopic().getConsumerGroupConfig().getWorkers();
        consumerGroup = topic.getTopic().getConsumerGroupConfig().getGroupName();
        executorService = Executors.newFixedThreadPool(totalWorkers);
        IntStream.range(0, totalWorkers).forEach(index -> {
          String consumerId = consumerGroup +"::"+ topic.getTopic().getName()+"::"+Integer.toString(index + 1);
          multithreadedConsumer = new MultithreadedConsumer(consumerId,
                  consumerGroup, consumerProperties, topic.getTopic(), buffer, pluginMetrics);
          consumers.add(multithreadedConsumer);
          executorService.submit(multithreadedConsumer);
        });
      } catch (Exception e) {
        LOG.error("Failed to setup the Kafka Source Plugin.", e);
        kafkaWorkerThreadProcessingErrors.increment();
      }
    });
  }

  @Override
  public void stop() {
    LOG.info("Shutting down Consumers...");
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(
              sourceConfig.getTopics().get(0).getTopic().getConsumerGroupConfig().getThreadWaitingTime().toSeconds(), TimeUnit.MILLISECONDS)) {
        LOG.info("Consumer threads are waiting for shutting down...");
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      if (e.getCause() instanceof InterruptedException) {
        LOG.error("Interrupted during consumer shutdown, exiting uncleanly...");
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    LOG.info("Consumer shutdown successfully...");
  }

  private Properties getConsumerProperties(TopicsConfig topicConfig) {
    Properties properties = new Properties();
    try {
      properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
              topicConfig.getTopic().getConsumerGroupConfig().getAutoCommitInterval().toSecondsPart());
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              topicConfig.getTopic().getConsumerGroupConfig().getAutoOffsetReset());
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
              topicConfig.getTopic().getConsumerGroupConfig().getAutoCommit());
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getTopic().getConsumerGroupConfig().getGroupId());
      setPropertiesForSchemaType(topicConfig, properties, topicConfig.getTopic().getSchemaConfig().getSchemaType());
    } catch (Exception e) {
      LOG.error("Unable to create the Kafka Consumer from the given configurations.", e);
    }

    return properties;
  }

  private void setPropertiesForSchemaType(TopicsConfig topicConfig, Properties properties, String schemaType) {

    if (schemaType.equalsIgnoreCase(MessageFormat.JSON.toString())) {
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              topicConfig.getTopic().getSchemaConfig().getKeyDeserializer());
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    } else if (schemaType.equalsIgnoreCase(MessageFormat.PLAINTEXT.toString())) {
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              topicConfig.getTopic().getSchemaConfig().getKeyDeserializer());
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              topicConfig.getTopic().getSchemaConfig().getValueDeserializer());
    }
  }
}
