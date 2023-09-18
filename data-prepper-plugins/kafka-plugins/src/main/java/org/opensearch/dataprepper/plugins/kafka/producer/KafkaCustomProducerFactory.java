package org.opensearch.dataprepper.plugins.kafka.producer;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.service.SchemaService;
import org.opensearch.dataprepper.plugins.kafka.service.TopicService;
import org.opensearch.dataprepper.plugins.kafka.sink.DLQSink;
import org.opensearch.dataprepper.plugins.kafka.util.RestUtils;
import org.opensearch.dataprepper.plugins.kafka.util.SinkPropertyConfigurer;

import java.util.Objects;
import java.util.Properties;

public class KafkaCustomProducerFactory {

    public KafkaCustomProducer createProducer(final KafkaProducerConfig kafkaSinkConfig, final PluginFactory pluginFactory, final PluginSetting pluginSetting,
                                              final ExpressionEvaluator expressionEvaluator, final SinkContext sinkContext) {
        prepareTopicAndSchema(kafkaSinkConfig);
        Properties properties = SinkPropertyConfigurer.getProducerProperties(kafkaSinkConfig);
        properties = Objects.requireNonNull(properties);
        return new KafkaCustomProducer(new org.apache.kafka.clients.producer.KafkaProducer<>(properties),
            kafkaSinkConfig, new DLQSink(pluginFactory, kafkaSinkConfig, pluginSetting),
            expressionEvaluator, Objects.nonNull(sinkContext) ? sinkContext.getTagsTargetKey() : null);
    }

    private void prepareTopicAndSchema(final KafkaProducerConfig kafkaSinkConfig) {
        checkTopicCreationCriteriaAndCreateTopic(kafkaSinkConfig);
        final SchemaConfig schemaConfig = kafkaSinkConfig.getSchemaConfig();
        if (schemaConfig != null) {
            if (schemaConfig.isCreate()) {
                final RestUtils restUtils = new RestUtils(schemaConfig);
                final String topic = kafkaSinkConfig.getTopics().get(0).getName();
                final SchemaService schemaService = new SchemaService.SchemaServiceBuilder()
                    .getRegisterationAndCompatibilityService(topic, kafkaSinkConfig.getSerdeFormat(),
                        restUtils, schemaConfig).build();
                schemaService.registerSchema(topic);
            }

        }

    }

    private void checkTopicCreationCriteriaAndCreateTopic(final KafkaProducerConfig kafkaSinkConfig) {
        final TopicConfig topic = kafkaSinkConfig.getTopics().get(0);
        if (topic.isCreate()) {
            final TopicService topicService = new TopicService(kafkaSinkConfig);
            topicService.createTopic(kafkaSinkConfig.getTopics().get(0).getName(), topic.getNumberOfPartions(), topic.getReplicationFactor());
            topicService.closeAdminClient();
        }


    }
}