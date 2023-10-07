/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducer;
import org.opensearch.dataprepper.plugins.kafka.producer.ProducerWorker;
import org.opensearch.dataprepper.plugins.kafka.service.SchemaService;
import org.opensearch.dataprepper.plugins.kafka.service.TopicService;
import org.opensearch.dataprepper.plugins.kafka.util.RestUtils;
import org.opensearch.dataprepper.plugins.kafka.util.SinkPropertyConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation class of kafka--sink plugin. It is responsible for receive the collection of
 * {@link Event} and produce it to different kafka topics.
 */
@DataPrepperPlugin(name = "kafka", pluginType = Sink.class, pluginConfigurationType = KafkaSinkConfig.class)
public class KafkaSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

    private final KafkaSinkConfig kafkaSinkConfig;

    private volatile boolean sinkInitialized;

    private static final Integer totalWorkers = 1;

    private ProducerWorker producerWorker;

    private ExecutorService executorService;

    private final PluginFactory pluginFactory;

    private final PluginSetting pluginSetting;

    private final ExpressionEvaluator expressionEvaluator;

    private final Lock reentrantLock;

    private final SinkContext sinkContext;


    @DataPrepperPluginConstructor
    public KafkaSink(final PluginSetting pluginSetting, final KafkaSinkConfig kafkaSinkConfig, final PluginFactory pluginFactory,
                     final ExpressionEvaluator expressionEvaluator, final SinkContext sinkContext) {
        super(pluginSetting);
        this.pluginSetting = pluginSetting;
        this.kafkaSinkConfig = kafkaSinkConfig;
        this.pluginFactory = pluginFactory;
        this.expressionEvaluator = expressionEvaluator;
        reentrantLock = new ReentrantLock();
        this.sinkContext = sinkContext;


    }


    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize kafka-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize kafka-sink plugin.");
            this.shutdown();
            throw e;
        }
    }

    private void doInitializeInternal() {
        executorService = Executors.newFixedThreadPool(totalWorkers);
        sinkInitialized = Boolean.TRUE;
    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {
        reentrantLock.lock();
        if (records.isEmpty()) {
            return;
        }
        try {
            prepareTopicAndSchema();
            final KafkaCustomProducer producer = createProducer();
            records.forEach(record -> {
                producerWorker = new ProducerWorker(producer, record);
                executorService.submit(producerWorker);
            });

        } catch (Exception e) {
            LOG.error("Failed to setup the Kafka sink Plugin.", e);
            throw new RuntimeException(e.getMessage());
        }
        reentrantLock.unlock();
    }

    private void prepareTopicAndSchema() {
        checkTopicCreationCriteriaAndCreateTopic();
        final SchemaConfig schemaConfig = kafkaSinkConfig.getSchemaConfig();
        if (schemaConfig != null) {
            if (schemaConfig.isCreate()) {
                final RestUtils restUtils = new RestUtils(schemaConfig);
                final String topic = kafkaSinkConfig.getTopic().getName();
                final SchemaService schemaService = new SchemaService.SchemaServiceBuilder()
                        .getRegisterationAndCompatibilityService(topic, kafkaSinkConfig.getSerdeFormat(),
                                restUtils, schemaConfig).build();
                schemaService.registerSchema(topic);
            }

        }

    }

    private void checkTopicCreationCriteriaAndCreateTopic() {
        final TopicConfig topic = kafkaSinkConfig.getTopic();
        if (topic.isCreate()) {
            final TopicService topicService = new TopicService(kafkaSinkConfig);
            topicService.createTopic(kafkaSinkConfig.getTopic().getName(), topic.getNumberOfPartions(), topic.getReplicationFactor());
            topicService.closeAdminClient();
        }


    }

    public KafkaCustomProducer createProducer() {
        Properties properties = SinkPropertyConfigurer.getProducerProperties(kafkaSinkConfig);
        properties = Objects.requireNonNull(properties);
        return new KafkaCustomProducer(new KafkaProducer<>(properties),
                kafkaSinkConfig, new DLQSink(pluginFactory, kafkaSinkConfig, pluginSetting),
                expressionEvaluator, Objects.nonNull(sinkContext) ? sinkContext.getTagsTargetKey() : null);
    }


    @Override
    public void shutdown() {
        try {
            if (!executorService.awaitTermination(
                    calculateLongestThreadWaitingTime(), TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
            LOG.info("Sink threads are waiting for shutting down...");
        } catch (InterruptedException e) {
            if (e.getCause() instanceof InterruptedException) {
                LOG.error("Interrupted during sink shutdown, exiting uncleanly...");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        super.shutdown();
        LOG.info("Producer shutdown successfully...");
    }

    private long calculateLongestThreadWaitingTime() {
        return kafkaSinkConfig.getThreadWaitTime();
    }


    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

}

