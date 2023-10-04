package org.opensearch.dataprepper.plugins.kafka.producer;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumerFactory;
import org.opensearch.dataprepper.plugins.kafka.service.SchemaService;
import org.opensearch.dataprepper.plugins.kafka.service.TopicService;
import org.opensearch.dataprepper.plugins.kafka.sink.DLQSink;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafka.util.RestUtils;
import org.opensearch.dataprepper.plugins.kafka.util.SinkPropertyConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class KafkaCustomProducerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCustomConsumerFactory.class);

    public KafkaCustomProducer createProducer(final KafkaProducerConfig kafkaProducerConfig, final PluginFactory pluginFactory, final PluginSetting pluginSetting,
                                              final ExpressionEvaluator expressionEvaluator, final SinkContext sinkContext) {
        prepareTopicAndSchema(kafkaProducerConfig);
        Properties properties = SinkPropertyConfigurer.getProducerProperties(kafkaProducerConfig);
        KafkaSecurityConfigurer.setAuthProperties(properties, kafkaProducerConfig, LOG);
        properties = Objects.requireNonNull(properties);
        return new KafkaCustomProducer(new org.apache.kafka.clients.producer.KafkaProducer<>(properties),
            kafkaProducerConfig, new DLQSink(pluginFactory, kafkaProducerConfig, pluginSetting),
            expressionEvaluator, Objects.nonNull(sinkContext) ? sinkContext.getTagsTargetKey() : null);
    }
    private void prepareTopicAndSchema(final KafkaProducerConfig kafkaProducerConfig) {
        checkTopicCreationCriteriaAndCreateTopic(kafkaProducerConfig);
        final SchemaConfig schemaConfig = kafkaProducerConfig.getSchemaConfig();
        if (schemaConfig != null) {
            if (schemaConfig.isCreate()) {
                final RestUtils restUtils = new RestUtils(schemaConfig);
                final String topic = kafkaProducerConfig.getTopic().getName();
                final SchemaService schemaService = new SchemaService.SchemaServiceBuilder()
                    .getRegisterationAndCompatibilityService(topic, kafkaProducerConfig.getSerdeFormat(),
                        restUtils, schemaConfig).build();
                schemaService.registerSchema(topic);
            }

        }

    }

    private void checkTopicCreationCriteriaAndCreateTopic(final KafkaProducerConfig kafkaProducerConfig) {
        final TopicConfig topic = kafkaProducerConfig.getTopic();
        if (!topic.isCreate()) {
            final TopicService topicService = new TopicService(kafkaProducerConfig);
            topicService.createTopic(kafkaProducerConfig.getTopic().getName(), topic.getNumberOfPartions(), topic.getReplicationFactor());
            topicService.closeAdminClient();
        }


    }
}