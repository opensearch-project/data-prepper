/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaPullEngineTest {

    @Mock
    private KafkaPullEngineConfig config;

    @Mock
    private TopicManager topicManager;

    private String topicName;
    private String key;
    private byte[] document;
    private int partitionCount;

    @BeforeEach
    void setUp() {
        topicName = UUID.randomUUID().toString();
        key = UUID.randomUUID().toString();
        document = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        partitionCount = 5;

        when(config.getBootstrapServers()).thenReturn(List.of("localhost:9092"));
    }

    private KafkaPullEngine createObjectUnderTest() {
        return new KafkaPullEngine(config, topicManager);
    }

    @Test
    void initialize_delegates_topic_creation_to_topic_manager() {
        final KafkaPullEngine objectUnderTest = createObjectUnderTest();

        try (final MockedConstruction<KafkaProducer> producerConstruction = mockConstruction(KafkaProducer.class)) {
            objectUnderTest.initialize(topicName, partitionCount);
        }

        verify(topicManager).createTopicWithPartitions(topicName, partitionCount);
    }

    @Test
    void write_sends_record_to_correct_partition() {
        final KafkaPullEngine objectUnderTest = createObjectUnderTest();

        try (final MockedConstruction<KafkaProducer> producerConstruction = mockConstruction(KafkaProducer.class)) {
            objectUnderTest.initialize(topicName, partitionCount);

            final KafkaProducer<String, byte[]> mockProducer = producerConstruction.constructed().get(0);

            objectUnderTest.write(0, key, document);

            @SuppressWarnings("unchecked")
            final ArgumentCaptor<ProducerRecord<String, byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
            verify(mockProducer).send(captor.capture(), any());

            final ProducerRecord<String, byte[]> record = captor.getValue();
            assertThat(record.topic(), equalTo(topicName));
            assertThat(record.partition(), equalTo(0));
            assertThat(record.key(), equalTo(key));
            assertThat(record.value(), equalTo(document));
        }
    }

    @Test
    void flush_delegates_to_producer() {
        final KafkaPullEngine objectUnderTest = createObjectUnderTest();

        try (final MockedConstruction<KafkaProducer> producerConstruction = mockConstruction(KafkaProducer.class)) {
            objectUnderTest.initialize(topicName, partitionCount);

            final KafkaProducer<String, byte[]> mockProducer = producerConstruction.constructed().get(0);
            objectUnderTest.flush();

            verify(mockProducer).flush();
        }
    }

    @Test
    void shutdown_closes_producer() {
        final KafkaPullEngine objectUnderTest = createObjectUnderTest();

        try (final MockedConstruction<KafkaProducer> producerConstruction = mockConstruction(KafkaProducer.class)) {
            objectUnderTest.initialize(topicName, partitionCount);

            final KafkaProducer<String, byte[]> mockProducer = producerConstruction.constructed().get(0);
            objectUnderTest.shutdown();

            verify(mockProducer).close();
        }
    }
}
