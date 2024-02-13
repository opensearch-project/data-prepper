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
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducer;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducerFactory;
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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaSinkTest {
    @Mock
    KafkaCustomProducer kafkaCustomProducer;

    KafkaSinkConfig kafkaSinkConfig;


    ExecutorService executorService;

    @Mock
    PluginSetting pluginSetting;

    @Mock
    PluginMetrics pluginMetrics;

    @Mock
    FutureTask futureTask;


    Event event;

    private static final Integer totalWorkers = 1;

    MockedStatic<Executors> executorsMockedStatic;

    @Mock
    private PluginFactory pluginFactoryMock;

    Properties props;

    @Mock
    SinkContext sinkContext;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;


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
        executorsMockedStatic = mockStatic(Executors.class);
        props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9093");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
    }

    @AfterEach
    public void after() {
        executorsMockedStatic.close();
    }

    private KafkaSink createObjectUnderTest() {
        final KafkaSink objectUnderTest;
        try(final MockedConstruction<KafkaCustomProducerFactory> ignored = mockConstruction(KafkaCustomProducerFactory.class, (mock, context) -> {
           when(mock.createProducer(any(), any(), any(), any(), any(), anyBoolean())).thenReturn(kafkaCustomProducer);
        })) {
            objectUnderTest = new KafkaSink(pluginSetting, kafkaSinkConfig, pluginFactoryMock, pluginMetrics, mock(ExpressionEvaluator.class), sinkContext, awsCredentialsSupplier);
        }
        ReflectionTestUtils.setField(objectUnderTest, "executorService", executorService);
        return spy(objectUnderTest);

    }

    @Test
    public void doOutputTest() {
        ReflectionTestUtils.setField(kafkaSinkConfig, "schemaConfig", null);
        when(executorService.submit(any(ProducerWorker.class))).thenReturn(futureTask);
        final Collection records = Arrays.asList(new Record(event));
        final KafkaSink objectUnderTest = createObjectUnderTest();

        objectUnderTest.doOutput(records);

        verify(objectUnderTest).doOutput(records);
    }


    @Test
    public void doOutputExceptionTest() {
        final Collection records = Arrays.asList(new Record(event));
        when(executorService.submit(any(ProducerWorker.class))).thenThrow(new RuntimeException());
        final KafkaSink objectUnderTest = createObjectUnderTest();
        assertThrows(RuntimeException.class, () -> objectUnderTest.doOutput(records));
    }

    @Test
    public void doOutputEmptyRecordsTest() {
        final Collection records = Arrays.asList();
        final KafkaSink objectUnderTest = createObjectUnderTest();
        objectUnderTest.doOutput(records);
        verify(objectUnderTest).doOutput(records);

    }

    @Test
    public void shutdownTest() {
        final KafkaSink objectUnderTest = createObjectUnderTest();
        objectUnderTest.shutdown();
        verify(objectUnderTest).shutdown();
    }

    @Test
    public void shutdownExceptionTest() throws InterruptedException {
        final InterruptedException interruptedException = new InterruptedException();
        interruptedException.initCause(new InterruptedException());

        when(executorService.awaitTermination(
                1000L, TimeUnit.MILLISECONDS)).thenThrow(interruptedException);

        createObjectUnderTest().shutdown();
    }


    @Test
    public void doInitializeTest() {
        final KafkaSink objectUnderTest = createObjectUnderTest();
        objectUnderTest.doInitialize();
        verify(objectUnderTest).doInitialize();
    }

    @Test
    public void doInitializeNullPointerExceptionTest() {
        when(Executors.newFixedThreadPool(totalWorkers)).thenThrow(NullPointerException.class);
        final KafkaSink objectUnderTest = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> objectUnderTest.doInitialize());
    }


    @Test
    public void isReadyTest() {
        final KafkaSink objectUnderTest = createObjectUnderTest();
        ReflectionTestUtils.setField(objectUnderTest, "sinkInitialized", true);
        assertEquals(true, objectUnderTest.isReady());
    }

    @Test
    public void doOutputTestForAutoTopicCreate() {

        SinkTopicConfig topicConfig = mock(SinkTopicConfig.class);
        when(topicConfig.isCreateTopic()).thenReturn(true);

        SchemaConfig schemaConfig = mock(SchemaConfig.class);
        when(schemaConfig.isCreate()).thenReturn(true);
        when(schemaConfig.getRegistryURL()).thenReturn("http://localhost:8085");
        when(schemaConfig.getInlineSchema()).thenReturn("{ \"type\": \"record\", \"name\": \"Person\", \"fields\": [{ \"name\": \"Year\", \"type\": \"string\" } ] }");

        ReflectionTestUtils.setField(kafkaSinkConfig, "schemaConfig", schemaConfig);
        ReflectionTestUtils.setField(kafkaSinkConfig, "topic", topicConfig);

        when(executorService.submit(any(ProducerWorker.class))).thenReturn(futureTask);
        final Collection records = Arrays.asList(new Record(event));
        final KafkaSink objectUnderTest = createObjectUnderTest();

        assertThrows(RuntimeException.class, () -> objectUnderTest.doOutput(records));
    }
}
