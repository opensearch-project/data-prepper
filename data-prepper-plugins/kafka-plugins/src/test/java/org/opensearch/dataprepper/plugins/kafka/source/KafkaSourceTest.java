/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.OAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.MultithreadedConsumer;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceTest {
    @Mock
    private KafkaSource source;

    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExecutorService executorService;

    @Mock
    private SchemaConfig schemaConfig;

    @Mock
    private TopicConfig topicConfig;
    @Mock
    private PipelineDescription pipelineDescription;
    @Mock
    OAuthConfig oAuthConfig;
    @Mock
    PlainTextAuthConfig plainTextAuthConfig;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "my-topic";

    @BeforeEach
    void setUp() throws Exception {
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            sourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
            topicConfig = sourceConfig.getTopics().get(0);
            oAuthConfig = sourceConfig.getAuthConfig().getoAuthConfig();
            plainTextAuthConfig = sourceConfig.getAuthConfig().getPlainTextAuthConfig();
            schemaConfig = sourceConfig.getSchemaConfig();
        }
    }

    @Test
    void test_kafkaSource_start_execution_catch_block() {
        source = new KafkaSource(null, pluginMetrics, pipelineDescription);
        KafkaSource spySource = spy(source);
        Assertions.assertThrows(Exception.class, () -> spySource.start(any()));
    }

    @Test
    void test_kafkaSource_stop_execution() throws Exception {
        List<MultithreadedConsumer> consumers = buildKafkaSourceConsumer();
        source = new KafkaSource(sourceConfig, pluginMetrics,pipelineDescription);
        KafkaSource spySource = spy(source);
        ReflectionTestUtils.setField(spySource, "executorService", executorService);
        doCallRealMethod().when(spySource).stop();
        spySource.stop();
        verify(spySource).stop();
    }

    private List<MultithreadedConsumer> buildKafkaSourceConsumer() {
        List<MultithreadedConsumer> consumers = new ArrayList<>();
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        MultithreadedConsumer kafkaSourceConsumer = new MultithreadedConsumer(
                topicConfig.getGroupId(),
                topicConfig.getGroupId(),
                prop, null,sourceConfig, null, pluginMetrics,null);
        consumers.add(kafkaSourceConsumer);
        return consumers;
    }
}
