package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.Properties;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;


class MultithreadedConsumerTest {

    @Mock
    MultithreadedConsumer multithreadedConsumer;
    @Mock
    Properties properties;
    @Mock
    KafkaSourceConfig sourceConfig;
    @Mock
    TopicConfig topicConfig;
    @Mock
    Buffer<Record<Object>> buffer;
    @Mock
    PluginMetrics pluginMetrics;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    @BeforeEach
    void setUp() {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "DPKafkaProj-1");
        pluginMetrics = mock(PluginMetrics.class);
        topicConfig = mock(TopicConfig.class);
        when(topicConfig.getName()).thenReturn("test-topic-1");
    }

    private MultithreadedConsumer createObjectUnderTest(String consumerId,
                                                        String consumerGroupId,
                                                        String schema){
        return new MultithreadedConsumer(consumerId,
                consumerGroupId,
                properties,
                topicConfig,
                sourceConfig,
                buffer,
                pluginMetrics,
                schema);
    }
    @ParameterizedTest
    @ValueSource(strings = {"plaintext", "json", "avro"})
    void testRunWithParamters(String schemaType) {
        /*multithreadedConsumer = createObjectUnderTest("DPKafkaProj-1",
                "DPKafkaProj-1",
                schemaType);
        MultithreadedConsumer spySource = spy(multithreadedConsumer);
        doCallRealMethod().when(spySource).run();
        spySource.run();
        verify(spySource).run();*/
    }

    @Test
    void shutdownConsumer() {
    }
}