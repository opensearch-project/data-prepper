/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicProducerConfig;
import org.opensearch.dataprepper.plugins.kafka.service.TopicService;
import org.opensearch.dataprepper.plugins.kafka.service.TopicServiceFactory;
import org.opensearch.dataprepper.plugins.kafka.sink.DLQSink;

import java.util.Collections;
import java.util.Random;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaCustomProducerFactoryTest {
    private static final Random RANDOM = new Random();
    @Mock
    private SerializationFactory serializationFactory;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private TopicServiceFactory topicServiceFactory;

    @Mock
    private KafkaProducerConfig kafkaProducerConfig;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private SinkContext sinkContext;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private DLQSink dlqSink;

    @Mock
    private TopicProducerConfig topicProducerConfig;
    @Mock
    private EncryptionConfig encryptionConfig;

    @BeforeEach
    void setUp() {
        when(kafkaProducerConfig.getTopic()).thenReturn(topicProducerConfig);
        when(kafkaProducerConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        when(kafkaProducerConfig.getBootstrapServers()).thenReturn(Collections.singletonList(UUID.randomUUID().toString()));

        final Serializer serializer = mock(Serializer.class);
        when(serializationFactory.getSerializer(any())).thenReturn(serializer);
    }

    private KafkaCustomProducerFactory createObjectUnderTest() {
        return new KafkaCustomProducerFactory(serializationFactory, awsCredentialsSupplier, topicServiceFactory);
    }

    @Test
    void createProducer_does_not_create_TopicService_when_createTopic_is_false() {
        final KafkaCustomProducerFactory objectUnderTest = createObjectUnderTest();
        try(final MockedConstruction<KafkaProducer> ignored = mockConstruction(KafkaProducer.class)) {
            objectUnderTest.createProducer(kafkaProducerConfig, expressionEvaluator, sinkContext, pluginMetrics, dlqSink, false);
        }

        verify(topicServiceFactory, never()).createTopicService(any());
    }

    @Test
    void createProducer_creates_TopicService_and_creates_topic_when_createTopic_is_true() {
        final TopicService topicService = mock(TopicService.class);
        when(topicServiceFactory.createTopicService(kafkaProducerConfig)).thenReturn(topicService);

        final String topicName = UUID.randomUUID().toString();
        final int numberOfPartitions = RANDOM.nextInt(1000);
        final short replicationFactor = (short) RANDOM.nextInt(1000);
        when(topicProducerConfig.getName()).thenReturn(topicName);
        when(topicProducerConfig.getNumberOfPartitions()).thenReturn(numberOfPartitions);
        when(topicProducerConfig.getReplicationFactor()).thenReturn(replicationFactor);


        final KafkaCustomProducerFactory objectUnderTest = createObjectUnderTest();

        when(topicProducerConfig.isCreateTopic()).thenReturn(true);

        try(final MockedConstruction<KafkaProducer> ignored = mockConstruction(KafkaProducer.class)) {
            objectUnderTest.createProducer(kafkaProducerConfig, expressionEvaluator, sinkContext, pluginMetrics, dlqSink, false);
        }

        final InOrder inOrder = inOrder(topicService);
        inOrder.verify(topicService).createTopic(topicName, numberOfPartitions, replicationFactor, null);
        inOrder.verify(topicService).closeAdminClient();

    }

}