/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class PlainTextConsumerIT {

    private PluginMetrics pluginMetrics;
    @Mock
    TopicConfig topicConfig;
    @Mock
    private SchemaConfig schemaConfig;
    private KafkaSourceConfig kafkaSourceConfig;

    private KafkaSource kafkaSource;
    private Buffer<Record<Event>> buffer;

    @ClassRule
    public static final EmbeddedKafkaClusterSingleNode CLUSTER = new EmbeddedKafkaClusterSingleNode();

    @BeforeClass
    public static void createTopics() {
        CLUSTER.createTopic("test-IT-topic");
    }

    @Before
    public void configure() throws IOException {
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines-int.yaml").getFile());
        Object data = yaml.load(fileReader);
        if(data instanceof Map){
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            List<TopicConfig> topicConfigList = kafkaSourceConfig.getTopics();
            topicConfig = topicConfigList.get(0);
            schemaConfig = kafkaSourceConfig.getSchemaConfig();
        }
    }

    @Test
    public void consumeKafkaMessages_should_return_at_least_one_message() {
        produceTestMessages();
        kafkaSource.start(buffer);
    }

    private void produceTestMessages() {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            for (long i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>("test-IT-topic",
                        "hello" + i));
                Thread.sleep(1000L);
            }
            producer.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
