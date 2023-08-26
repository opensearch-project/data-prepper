/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.dataprepper.plugins.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.producer.ProducerWorker;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkasinkTest {


    KafkaSink kafkaSink;


    KafkaSinkConfig kafkaSinkConfig;


    ExecutorService executorService;

    @Mock
    PluginSetting pluginSetting;

    @Mock
    FutureTask futureTask;


    Event event;

    KafkaSink spySink;

    private static final Integer totalWorkers = 1;

    MockedStatic<Executors> executorsMockedStatic;

    @Mock
    private PluginFactory pluginFactoryMock;

    Properties props;

    @Mock
    SinkContext sinkContext;


    @BeforeEach
    void setUp() throws Exception {
        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines-sink.yaml").getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sinkeMap = (Map<String, Object>) logPipelineMap.get("sink");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sinkeMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            kafkaSinkConfig = mapper.readValue(reader, KafkaSinkConfig.class);
        }
        executorService = mock(ExecutorService.class);
        when(pluginSetting.getPipelineName()).thenReturn("Kafka-sink");
        event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        when(sinkContext.getTagsTargetKey()).thenReturn("tag");
        kafkaSink = new KafkaSink(pluginSetting, kafkaSinkConfig, pluginFactoryMock, mock(ExpressionEvaluator.class), sinkContext);
        spySink = spy(kafkaSink);
        executorsMockedStatic = mockStatic(Executors.class);
        props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9093");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        ReflectionTestUtils.setField(spySink, "executorService", executorService);


    }

    @AfterEach
    public void after() {
        executorsMockedStatic.close();
    }

    @Test
    public void doOutputTest() {
        ReflectionTestUtils.setField(kafkaSinkConfig, "schemaConfig", null);
        when(executorService.submit(any(ProducerWorker.class))).thenReturn(futureTask);
        final Collection records = Arrays.asList(new Record(event));
        spySink.doOutput(records);
        verify(spySink).doOutput(records);
    }


    @Test
    public void doOutputExceptionTest() {
        final Collection records = Arrays.asList(new Record(event));
        when(executorService.submit(any(ProducerWorker.class))).thenThrow(new RuntimeException());
        assertThrows(RuntimeException.class, () -> spySink.doOutput(records));
    }

    @Test
    public void doOutputEmptyRecordsTest() {
        final Collection records = Arrays.asList();
        spySink.doOutput(records);
        verify(spySink).doOutput(records);

    }

    @Test
    public void shutdownTest() {
        spySink.shutdown();
        verify(spySink).shutdown();
    }

    @Test
    public void shutdownExceptionTest() throws InterruptedException {
        final InterruptedException interruptedException = new InterruptedException();
        interruptedException.initCause(new InterruptedException());

        when(executorService.awaitTermination(
                1000L, TimeUnit.MILLISECONDS)).thenThrow(interruptedException);
        spySink.shutdown();

    }


    @Test
    public void doInitializeTest() {
        spySink.doInitialize();
        verify(spySink).doInitialize();
    }

    @Test
    public void doInitializeNullPointerExceptionTest() {
        when(Executors.newFixedThreadPool(totalWorkers)).thenThrow(NullPointerException.class);
        assertThrows(NullPointerException.class, () -> spySink.doInitialize());
    }


    @Test
    public void isReadyTest() {
        ReflectionTestUtils.setField(kafkaSink, "sinkInitialized", true);
        assertEquals(true, kafkaSink.isReady());
    }

    @Test
    public void doOutputTestForAutoTopicCreate() {

        TopicConfig topicConfig = mock(TopicConfig.class);
        when(topicConfig.isCreate()).thenReturn(true);

        SchemaConfig schemaConfig = mock(SchemaConfig.class);
        when(schemaConfig.isCreate()).thenReturn(true);
        when(schemaConfig.getRegistryURL()).thenReturn("http://localhost:8085");
        when(schemaConfig.getInlineSchema()).thenReturn("{ \"type\": \"record\", \"name\": \"Person\", \"fields\": [{ \"name\": \"Year\", \"type\": \"string\" } ] }");

        ReflectionTestUtils.setField(kafkaSinkConfig, "schemaConfig", schemaConfig);
        ReflectionTestUtils.setField(kafkaSinkConfig, "topic", topicConfig);

        when(executorService.submit(any(ProducerWorker.class))).thenReturn(futureTask);
        final Collection records = Arrays.asList(new Record(event));
        assertThrows(RuntimeException.class, () -> spySink.doOutput(records));
    }
}
