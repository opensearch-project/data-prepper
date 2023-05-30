/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import java.util.List;
import java.util.Properties;

import java.util.stream.IntStream;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
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
  private ExecutorService executorService;
  private static final String KAFKA_WORKER_THREAD_PROCESSING_ERRORS = "kafkaWorkerThreadProcessingErrors";
  private final Counter kafkaWorkerThreadProcessingErrors;
  private final PluginMetrics pluginMetrics;
  private String consumerGroupID;
  private String pipelineName;
  private static String schemaType = "";
  private MultithreadedConsumer multithreadedConsumer;
  private int totalWorkers;

  @DataPrepperPluginConstructor
  public KafkaSource(final KafkaSourceConfig sourceConfig, final PluginMetrics pluginMetrics,
                     final PipelineDescription pipelineDescription) {
    this.sourceConfig = sourceConfig;
    this.pluginMetrics = pluginMetrics;
    this.pipelineName = pipelineDescription.getPipelineName();
    this.kafkaWorkerThreadProcessingErrors = pluginMetrics.counter(KAFKA_WORKER_THREAD_PROCESSING_ERRORS);
  }

  @Override
  public void start(Buffer<Record<Object>> buffer) {

    sourceConfig.getTopics().forEach(topic -> {
      totalWorkers = 0;
      try {
        Properties consumerProperties = getConsumerProperties(topic);
        totalWorkers = topic.getWorkers();
        consumerGroupID = getGroupId(topic.getName());
        executorService = Executors.newFixedThreadPool(totalWorkers);
        IntStream.range(0, totalWorkers+1).forEach(index -> {
          String consumerId = consumerGroupID + "::" + Integer.toString(index + 1);
          multithreadedConsumer = new MultithreadedConsumer(consumerId,
                  consumerGroupID, consumerProperties, topic, sourceConfig, buffer, pluginMetrics, schemaType);
          executorService.submit(multithreadedConsumer);
        });
      } catch (Exception e) {
        LOG.error("Failed to setup the Kafka Source Plugin.", e);
        throw new RuntimeException();
      }
    });
  }

  @Override
  public void stop() {
    LOG.info("Shutting down Consumers...");
    executorService.shutdown();
    try {
     if (!executorService.awaitTermination(
              calculateLongestThreadWaitingTime(), TimeUnit.MILLISECONDS)) {
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

  private String getGroupId(String name) {
    return  pipelineName +"::"+ name;
  }
  private long calculateLongestThreadWaitingTime() {
    List<TopicConfig> topicsList = sourceConfig.getTopics();
    return topicsList.stream().
            map(
                    topics -> topics.getThreadWaitingTime().toSeconds()
            ).
            max(Comparator.comparingLong(time -> time)).
            orElse(1L);
  }

  private Properties getConsumerProperties(TopicConfig topicConfig) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
            topicConfig.getAutoCommitInterval().toSecondsPart());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            topicConfig.getAutoOffsetReset());
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            topicConfig.getAutoCommit());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, topicConfig.getGroupId());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
    return properties;
  }
}
