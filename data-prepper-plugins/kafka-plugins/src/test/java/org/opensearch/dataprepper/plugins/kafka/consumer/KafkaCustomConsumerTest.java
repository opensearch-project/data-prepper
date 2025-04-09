/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBufferConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaCustomConsumerTest {
    private static final String TOPIC_NAME = "topic1";

    @Mock
    private KafkaConsumer<String, Object> kafkaConsumer;

    private AtomicBoolean status;

    private Buffer<Record<Event>> buffer;

    @Mock
    private Buffer<Record<Event>> mockBuffer;

    @Mock
    private KafkaConsumerConfig sourceConfig;

    private ScheduledExecutorService callbackExecutor;
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private TopicConsumerConfig topicConfig;

    @Mock
    private KafkaTopicConsumerMetrics topicMetrics;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private PauseConsumePredicate pauseConsumePredicate;

    private KafkaCustomConsumer consumer;

    private ConsumerRecords consumerRecords;

    private final String TEST_PIPELINE_NAME = "test_pipeline";
    private AtomicBoolean shutdownInProgress;
    private final String testKey1 = "testkey1";
    private final String testKey2 = "testkey2";
    private final String testValue1 = "testValue1";
    private final String testValue2 = "testValue2";
    private final Map<String, Object> testMap1 = Map.of("key1", "value1", "key2", 2);
    private final Map<String, Object> testMap2 = Map.of("key3", "value3", "key4", false);
    private final String testJsonValue1 = "{ \"key1\": \"value1\", \"key2\": 2}";
    private final String testJsonValue2 = "{ \"key3\": \"value3\", \"key4\": false}";
    private final int testPartition = 0;
    private final int testJsonPartition = 1;
    private Counter counter;
    @Mock
    private Counter posCounter;
    @Mock
    private Counter negCounter;
    @Mock
    private Counter overflowCounter;
    private Duration delayTime;
    private double posCount;
    private double negCount;
    private double overflowCount;
    private boolean paused;
    private boolean resumed;
    private final String jsonOtelTrace = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"host.name\",\"value\":{\"stringValue\":\"d0f4b15ecc95\"}},{\"key\":\"os.type\",\"value\":{\"stringValue\":\"linux\"}},{\"key\":\"service.name\",\"value\":{\"stringValue\":\"frontend\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"go\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"1.35.0\"}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp\",\"version\":\"0.60.0\"},\"spans\":[{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"96f03f6cb92f7b90\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"HTTP GET\",\"kind\":3,\"startTimeUnixNano\":\"1742979825273178545\",\"endTimeUnixNano\":\"1742979825598900571\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8081/customer?customer=123\"}},{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"0.0.0.0\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"8081\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"66\"}}],\"status\":{}},{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"092c887b0c5d9440\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"HTTP GET\",\"kind\":3,\"startTimeUnixNano\":\"1742979825802476962\",\"endTimeUnixNano\":\"1742979825833572603\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8083/route?dropoff=115%2C277&pickup=215%2C831\"}},{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"0.0.0.0\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"8083\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"59\"}}],\"status\":{}},{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"8d897be526d813bc\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"HTTP GET\",\"kind\":3,\"startTimeUnixNano\":\"1742979825867387275\",\"endTimeUnixNano\":\"1742979825916602821\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8083/route?dropoff=115%2C277&pickup=518%2C647\"}},{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"0.0.0.0\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"8083\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"59\"}}],\"status\":{}},{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"571c73a657d9b262\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"HTTP GET\",\"kind\":3,\"startTimeUnixNano\":\"1742979825884139837\",\"endTimeUnixNano\":\"1742979825921450639\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8083/route?dropoff=115%2C277&pickup=690%2C883\"}},{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"0.0.0.0\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"8083\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"59\"}}],\"status\":{}},{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"9bafa9c874b84d68\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"HTTP GET\",\"kind\":3,\"startTimeUnixNano\":\"1742979825916840389\",\"endTimeUnixNano\":\"1742979825963427000\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8083/route?dropoff=115%2C277&pickup=390%2C561\"}},{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"0.0.0.0\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"8083\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"59\"}}],\"status\":{}},{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"fa4462a050e4dac5\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"HTTP GET\",\"kind\":3,\"startTimeUnixNano\":\"1742979825910084145\",\"endTimeUnixNano\":\"1742979825966741012\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8083/route?dropoff=115%2C277&pickup=98%2C372\"}},{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"0.0.0.0\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"8083\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"58\"}}],\"status\":{}},{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"76ef8f38a2304d1a\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"HTTP GET\",\"kind\":3,\"startTimeUnixNano\":\"1742979825921538245\",\"endTimeUnixNano\":\"1742979825970090244\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.url\",\"value\":{\"stringValue\":\"http://0.0.0.0:8083/route?dropoff=115%2C277&pickup=743%2C348\"}},{\"key\":\"net.peer.name\",\"value\":{\"stringValue\":\"0.0.0.0\"}},{\"key\":\"net.peer.port\",\"value\":{\"intValue\":\"8083\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"59\"}}],\"status\":{}},{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"4ce150ecd279e8c6\",\"parentSpanId\":\"\",\"flags\":256,\"name\":\"/dispatch\",\"kind\":2,\"startTimeUnixNano\":\"1742979825272961906\",\"endTimeUnixNano\":\"1742979825970176806\",\"attributes\":[{\"key\":\"http.method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"http.scheme\",\"value\":{\"stringValue\":\"http\"}},{\"key\":\"net.host.name\",\"value\":{\"stringValue\":\"10.208.39.111\"}},{\"key\":\"net.host.port\",\"value\":{\"intValue\":\"8080\"}},{\"key\":\"net.sock.peer.addr\",\"value\":{\"stringValue\":\"10.208.39.111\"}},{\"key\":\"net.sock.peer.port\",\"value\":{\"intValue\":\"38658\"}},{\"key\":\"user_agent.original\",\"value\":{\"stringValue\":\"curl/7.68.0\"}},{\"key\":\"http.target\",\"value\":{\"stringValue\":\"/dispatch\"}},{\"key\":\"net.protocol.version\",\"value\":{\"stringValue\":\"1.1\"}},{\"key\":\"http.route\",\"value\":{\"stringValue\":\"/dispatch\"}},{\"key\":\"http.response_content_length\",\"value\":{\"intValue\":\"40\"}},{\"key\":\"http.status_code\",\"value\":{\"intValue\":\"200\"}}],\"events\":[{\"timeUnixNano\":\"1742979825272991693\",\"name\":\"HTTP request received\",\"attributes\":[{\"key\":\"method\",\"value\":{\"stringValue\":\"GET\"}},{\"key\":\"url\",\"value\":{\"stringValue\":\"/dispatch?customer=123\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825273047474\",\"name\":\"Getting customer\",\"attributes\":[{\"key\":\"customer_id\",\"value\":{\"intValue\":\"123\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825598929868\",\"name\":\"Found customer\",\"attributes\":[{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825599001745\",\"name\":\"Finding nearest drivers\",\"attributes\":[{\"key\":\"location\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825801336801\",\"name\":\"Found drivers\",\"attributes\":[{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825801638049\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"215,831\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825801728428\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"273,569\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825801754231\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"783,785\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825833609478\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"228,784\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825862484670\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"447,941\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825867207590\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"518,647\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825884086918\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"690,883\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825909797901\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"98,372\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825916626681\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"390,561\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825921496181\",\"name\":\"Finding route\",\"attributes\":[{\"key\":\"pickup\",\"value\":{\"stringValue\":\"743,348\"}},{\"key\":\"dropoff\",\"value\":{\"stringValue\":\"115,277\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825970110830\",\"name\":\"Found routes\",\"attributes\":[{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]},{\"timeUnixNano\":\"1742979825970158119\",\"name\":\"Dispatch successful\",\"attributes\":[{\"key\":\"driver\",\"value\":{\"stringValue\":\"T789809C\"}},{\"key\":\"eta\",\"value\":{\"stringValue\":\"2m0s\"}},{\"key\":\"level\",\"value\":{\"stringValue\":\"info\"}}]}],\"status\":{}}]},{\"scope\":{\"name\":\"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc\",\"version\":\"0.60.0\"},\"spans\":[{\"traceId\":\"099ce04f04acea26f8191b2a900d92e1\",\"spanId\":\"165298d534f12949\",\"parentSpanId\":\"4ce150ecd279e8c6\",\"flags\":256,\"name\":\"driver.DriverService/FindNearest\",\"kind\":3,\"startTimeUnixNano\":\"1742979825599035703\",\"endTimeUnixNano\":\"1742979825801312513\",\"attributes\":[{\"key\":\"rpc.service\",\"value\":{\"stringValue\":\"driver.DriverService\"}},{\"key\":\"rpc.method\",\"value\":{\"stringValue\":\"FindNearest\"}},{\"key\":\"rpc.system\",\"value\":{\"stringValue\":\"grpc\"}},{\"key\":\"net.sock.peer.addr\",\"value\":{\"stringValue\":\"127.0.0.1\"}},{\"key\":\"net.sock.peer.port\",\"value\":{\"intValue\":\"8082\"}},{\"key\":\"rpc.grpc.status_code\",\"value\":{\"intValue\":\"0\"}}],\"status\":{}}]}],\"schemaUrl\":\"https://opentelemetry.io/schemas/1.26.0\"}]}";
    private final String jsonOtelTraceWithError = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"python\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"opentelemetry\"}},{\"key\":\"telemetry.sdk.version\",\"value\":{\"stringValue\":\"1.31.1\"}},{\"key\":\"service.name\",\"value\":{\"stringValue\":\"my-service\"}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"__main__\"},\"spans\":[{\"traceId\":\"20e7534679d00831a0687c03ceb2f650\",\"spanId\":\"707a81c4bcfadcb9\",\"parentSpanId\":\"df1b295f6e239e4d\",\"flags\":256,\"name\":\"faulty_function\",\"kind\":1,\"startTimeUnixNano\":\"1743513207034428905\",\"endTimeUnixNano\":\"1743513207035602498\",\"events\":[{\"timeUnixNano\":\"1743513207035587881\",\"name\":\"exception\",\"attributes\":[{\"key\":\"exception.type\",\"value\":{\"stringValue\":\"ValueError\"}},{\"key\":\"exception.message\",\"value\":{\"stringValue\":\"Error!\"}},{\"key\":\"exception.stacktrace\",\"value\":{\"stringValue\":\"Traceback (most recent call last):\\n  File \\\"/home/ilys/Programming/work/trace_error/venv/lib/python3.13/site-packages/opentelemetry/trace/__init__.py\\\", line 587, in use_span\\n    yield span\\n  File \\\"/home/ilys/Programming/work/trace_error/venv/lib/python3.13/site-packages/opentelemetry/sdk/trace/__init__.py\\\", line 1105, in start_as_current_span\\n    yield span\\n  File \\\"/home/ilys/Programming/work/trace_error/./TraceGen.py\\\", line 29, in main\\n    faulty_function()\\n    ~~~~~~~~~~~~~~~^^\\n  File \\\"/home/ilys/Programming/work/trace_error/./TraceGen.py\\\", line 22, in faulty_function\\n    raise ValueError(\\\"Error!\\\")\\nValueError: Error!\\n\"}},{\"key\":\"exception.escaped\",\"value\":{\"stringValue\":\"False\"}}]}],\"status\":{\"message\":\"ValueError: Error!\",\"code\":2}},{\"traceId\":\"20e7534679d00831a0687c03ceb2f650\",\"spanId\":\"df1b295f6e239e4d\",\"parentSpanId\":\"\",\"flags\":256,\"name\":\"main\",\"kind\":1,\"startTimeUnixNano\":\"1743513207034388924\",\"endTimeUnixNano\":\"1743513207035646200\",\"status\":{}}]}]}]}";

    @BeforeEach
    public void setUp() throws JsonProcessingException {
        delayTime = Duration.ofMillis(10);
        paused = false;
        resumed = false;
        kafkaConsumer = mock(KafkaConsumer.class);
        topicMetrics = mock(KafkaTopicConsumerMetrics.class);
        counter = mock(Counter.class);
        posCounter = mock(Counter.class);
        mockBuffer = mock(Buffer.class);
        negCounter = mock(Counter.class);
        overflowCounter = mock(Counter.class);
        topicConfig = mock(TopicConsumerConfig.class);
        when(topicMetrics.getNumberOfPositiveAcknowledgements()).thenReturn(posCounter);
        when(topicMetrics.getNumberOfNegativeAcknowledgements()).thenReturn(negCounter);
        when(topicMetrics.getNumberOfBufferSizeOverflows()).thenReturn(overflowCounter);
        when(topicMetrics.getNumberOfRecordsCommitted()).thenReturn(counter);
        when(topicMetrics.getNumberOfDeserializationErrors()).thenReturn(counter);
        when(topicMetrics.getNumberOfInvalidTimeStamps()).thenReturn(counter);
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(kafkaConsumer.committed(any(TopicPartition.class))).thenReturn(null);

        doAnswer((i) -> {
            paused = true;
            return null;
        }).when(kafkaConsumer).pause(any());

        doAnswer((i) -> {
            resumed = true;
            return null;
        }).when(kafkaConsumer).resume(any());

        doAnswer((i) -> {
            posCount += 1.0;
            return null;
        }).when(posCounter).increment();
        doAnswer((i) -> {
            negCount += 1.0;
            return null;
        }).when(negCounter).increment();
        doAnswer((i) -> {
            overflowCount += 1.0;
            return null;
        }).when(overflowCounter).increment();
        doAnswer((i)-> {return posCount;}).when(posCounter).count();
        doAnswer((i)-> {return negCount;}).when(negCounter).count();
        callbackExecutor = Executors.newScheduledThreadPool(2); 
        acknowledgementSetManager = new DefaultAcknowledgementSetManager(callbackExecutor, Duration.ofMillis(2000));

        sourceConfig = mock(KafkaConsumerConfig.class);
        buffer = getBuffer();
        shutdownInProgress = new AtomicBoolean(false);
        when(topicConfig.getName()).thenReturn(TOPIC_NAME);
    }

    public KafkaCustomConsumer createObjectUnderTestWithMockBuffer(String schemaType) {
        return new KafkaCustomConsumer(kafkaConsumer, shutdownInProgress, mockBuffer, sourceConfig, topicConfig, schemaType,
                acknowledgementSetManager, null, topicMetrics, pauseConsumePredicate);
    }

    public KafkaCustomConsumer createObjectUnderTest(String schemaType, boolean acknowledgementsEnabled) {
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(acknowledgementsEnabled);
        return new KafkaCustomConsumer(kafkaConsumer, shutdownInProgress, buffer, sourceConfig, topicConfig, schemaType,
                acknowledgementSetManager, null, topicMetrics, pauseConsumePredicate);
    }

    private BlockingBuffer<Record<Event>> getBuffer() throws JsonProcessingException {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 10);
        integerHashMap.put("batch_size", 10);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(integerHashMap);
        BlockingBufferConfig blockingBufferConfig = objectMapper.readValue(json, BlockingBufferConfig.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(blockingBufferConfig, pipelineDescription);
    }

    @Test
    public void testGetRecordTimeStamp() {
        ConsumerRecord<String, Object> consumerRecord1 = mock(ConsumerRecord.class);
        ConsumerRecord<String, Object> consumerRecord2 = mock(ConsumerRecord.class);
        ConsumerRecord<String, Object> consumerRecord3 = mock(ConsumerRecord.class);
        consumer = createObjectUnderTestWithMockBuffer("plaintext");
        long nowMs = Instant.now().toEpochMilli();
        long timestamp1 = nowMs - 5;
        when(consumerRecord1.timestamp()).thenReturn(timestamp1);
        when(consumerRecord1.partition()).thenReturn(1);
        assertThat(consumer.getRecordTimeStamp(consumerRecord1, nowMs), equalTo(timestamp1));
        long timestamp2 = nowMs + 5;
        when(consumerRecord2.timestamp()).thenReturn(timestamp2);
        when(consumerRecord2.partition()).thenReturn(1);
        assertThat(consumer.getRecordTimeStamp(consumerRecord2, nowMs), equalTo(timestamp1));
        long timestamp3 = nowMs + 10;
        when(consumerRecord3.timestamp()).thenReturn(timestamp3);
        when(consumerRecord3.partition()).thenReturn(2);
        assertThat(consumer.getRecordTimeStamp(consumerRecord3, nowMs), equalTo(nowMs));
    }

    @Test
    public void testBufferOverflowPauseResume() throws InterruptedException, Exception {
        when(topicConfig.getMaxPollInterval()).thenReturn(Duration.ofMillis(4000));
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        doAnswer((i) -> {
            if (!paused && !resumed)
                throw new SizeOverflowException("size overflow");
            buffer.writeAll(i.getArgument(0), i.getArgument(1));
            return null;
        }).when(mockBuffer).writeAll(any(), anyInt());

        doAnswer((i) -> {
            if (paused && !resumed)
                return List.of();
            return consumerRecords;
        }).when(kafkaConsumer).poll(any(Duration.class));
        consumer = createObjectUnderTestWithMockBuffer("plaintext");
        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e){}
        assertTrue(paused);
        assertTrue(resumed);

        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 2L);
        });
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 2L);

        for (Record<Event> record : bufferedRecords) {
            Event event = record.getData();
            String value1 = event.get(testKey1, String.class);
            String value2 = event.get(testKey2, String.class);
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
        }
    }

    @Test
    public void testPlainTextConsumeRecords() throws InterruptedException {
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("plaintext", false);

        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e){}
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 2L);
        });
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 2L);


        for (Record<Event> record : bufferedRecords) {
            Event event = record.getData();
            String value1 = event.get(testKey1, String.class);
            String value2 = event.get(testKey2, String.class);
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
        }
    }

    @Test
    public void testPlainTextConsumeRecordsWithAcknowledgements() throws InterruptedException {
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("plaintext", true);

        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e){}
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record : bufferedRecords) {
            Event event = record.getData();
            String value1 = event.get(testKey1, String.class);
            String value2 = event.get(testKey2, String.class);
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
            event.getEventHandle().release(true);
        }
        // Wait for acknowledgement callback function to run
        await().atMost(delayTime.plusMillis(5000))
                .until(() -> {
                    return consumer.getTopicMetrics().getNumberOfPositiveAcknowledgements().count() == 1.0;
                });

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 2L);
        });
        // This counter should not be incremented with acknowledgements
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 0L);
    }

    @Test
    public void testPlainTextConsumeRecordsWithNegativeAcknowledgements() throws InterruptedException {
        String topic = topicConfig.getName();
        consumerRecords = createPlainTextRecords(topic, 0L);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("plaintext", true);

        try {
            consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
            consumer.consumeRecords();
        } catch (Exception e) {
        }
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record : bufferedRecords) {
            Event event = record.getData();
            String value1 = event.get(testKey1, String.class);
            String value2 = event.get(testKey2, String.class);
            assertTrue(value1 != null || value2 != null);
            if (value1 != null) {
                Assertions.assertEquals(value1, testValue1);
            }
            if (value2 != null) {
                Assertions.assertEquals(value2, testValue2);
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
            event.getEventHandle().release(false);
        }
        // Wait for acknowledgement callback function to run
        await().atMost(delayTime.plusMillis(5000))
                .until(() -> {
                    return consumer.getTopicMetrics().getNumberOfNegativeAcknowledgements().count() == 1.0;
                });

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);
    }

    @Test
    public void testJsonConsumeRecords() throws InterruptedException, Exception {
        String topic = topicConfig.getName();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        consumerRecords = createJsonRecords(topic);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("json", false);

        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testJsonPartition)));
        consumer.consumeRecords();
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(consumerRecords.count(), bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testJsonPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 102L);
        });
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 2L);

        for (Record<Event> record : bufferedRecords) {
            Event event = record.getData();
            Map<String, Object> eventMap = event.toMap();
            String kafkaKey = event.get("kafka_key", String.class);
            assertTrue(kafkaKey.equals(testKey1) || kafkaKey.equals(testKey2));
            if (kafkaKey.equals(testKey1)) {
                testMap1.forEach((k, v) -> assertThat(eventMap, hasEntry(k, v)));
            }
            if (kafkaKey.equals(testKey2)) {
                testMap2.forEach((k, v) -> assertThat(eventMap, hasEntry(k, v)));
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
        }
    }

    @Test
    public void testJsonOtelTraceConsumeRecords() throws InterruptedException, Exception {
        String topic = topicConfig.getName();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON_OTEL_TRACE);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        consumerRecords = createJsonOtelRecords(topic);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("json_otel_trace", false);

        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
        consumer.consumeRecords();
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(9, bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 101L);
        });
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 1L);

        for (Record<Event> record : bufferedRecords) {
            assertEquals(JacksonSpan.class, record.getData().getClass());
            Span span = (Span) record.getData();
            if (span.getSpanId().equals("96f03f6cb92f7b90")) {
                assertEquals("099ce04f04acea26f8191b2a900d92e1", span.getTraceId());
                assertEquals("frontend", span.getServiceName());
                assertEquals(8081, span.getAttributes().get("net.peer.port"));
                assertEquals(325722026, span.getDurationInNanos());
                assertEquals("4ce150ecd279e8c6", span.getParentSpanId());
            } else if (span.getSpanId().equals("4ce150ecd279e8c6")) {
                assertEquals("099ce04f04acea26f8191b2a900d92e1", span.getTraceId());
                assertEquals("frontend", span.getServiceName());
                assertEquals("10.208.39.111", span.getAttributes().get("net.host.name"));
                assertEquals(697214900, span.getDurationInNanos());
                assertEquals("", span.getParentSpanId());
                assertEquals(697214900, span.getTraceGroupFields().getDurationInNanos());
            }

        }
    }

    @Test
    public void testJsonOtelTraceConsumeRecordsWithErorr() throws InterruptedException, Exception {
        String topic = topicConfig.getName();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON_OTEL_TRACE);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);
        consumerRecords = createJsonOtelRecordsWithErrors(topic);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer = createObjectUnderTest("json_otel_trace", false);

        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testPartition)));
        consumer.consumeRecords();
        final Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(2, bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(offsetAndMetadata.offset(), 101L);
        });
        Assertions.assertEquals(consumer.getNumRecordsCommitted(), 1L);

        for (Record<Event> record : bufferedRecords) {
            assertEquals(JacksonSpan.class, record.getData().getClass());
            Span span = (Span) record.getData();
            if (span.getSpanId().equals("707a81c4bcfadcb9")) {
                assertEquals("20e7534679d00831a0687c03ceb2f650", span.getTraceId());
                assertEquals("my-service", span.getServiceName());
                assertEquals(2, span.getAttributes().get("status.code"));
                assertEquals("ValueError: Error!", span.getAttributes().get("status.message"));
                assertEquals(1173593, span.getDurationInNanos());
                assertEquals("df1b295f6e239e4d", span.getParentSpanId());
            } else if (span.getSpanId().equals("df1b295f6e239e4d")) {
                assertEquals("20e7534679d00831a0687c03ceb2f650", span.getTraceId());
                assertEquals("my-service", span.getServiceName());
                assertEquals("main", span.getTraceGroup());
                assertEquals(0, span.getAttributes().get("status.code"));
                assertNull(span.getAttributes().get("status.message"));
                assertEquals(1257276, span.getDurationInNanos());
                assertEquals("", span.getParentSpanId());
                assertEquals(1257276, span.getTraceGroupFields().getDurationInNanos());
            }
        }
    }

    @Test
    public void testJsonDeserializationErrorWithAcknowledgements() throws Exception {
        String topic = topicConfig.getName();
        final ObjectMapper mapper = new ObjectMapper();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);

        consumer = createObjectUnderTest("json", true);
        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testJsonPartition)));

        // Send one json record
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>(topic, testJsonPartition, 100L, testKey1, mapper.convertValue(testMap1, JsonNode.class));
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record1));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        // Send non-json record that results in deser exception
        RecordDeserializationException exc = new RecordDeserializationException(new TopicPartition(topic, testJsonPartition),
                101L, "Deserializedation exception", new JsonParseException("Json parse exception"));
        when(kafkaConsumer.poll(any(Duration.class))).thenThrow(exc);
        consumer.consumeRecords();

        // Send one more json record
        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 102L, testKey2,
                mapper.convertValue(testMap2, JsonNode.class));
        records.clear();
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record2));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(2, bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record : bufferedRecords) {
            Event event = record.getData();
            Map<String, Object> eventMap = event.toMap();
            String kafkaKey = event.get("kafka_key", String.class);
            assertTrue(kafkaKey.equals(testKey1) || kafkaKey.equals(testKey2));
            if (kafkaKey.equals(testKey1)) {
                testMap1.forEach((k, v) -> assertThat(eventMap, hasEntry(k, v)));
            }
            if (kafkaKey.equals(testKey2)) {
                testMap2.forEach((k, v) -> assertThat(eventMap, hasEntry(k, v)));
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
            event.getEventHandle().release(true);
        }
        // Wait for acknowledgement callback function to run
        try {
            Thread.sleep(100);
        } catch (Exception e){}

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testJsonPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(103L, offsetAndMetadata.offset());
        });
    }

    @Test
    public void testAwsGlueErrorWithAcknowledgements() throws Exception {
        String topic = topicConfig.getName();
        final ObjectMapper mapper = new ObjectMapper();
        when(topicConfig.getSerdeFormat()).thenReturn(MessageFormat.JSON);
        when(topicConfig.getKafkaKeyMode()).thenReturn(KafkaKeyMode.INCLUDE_AS_FIELD);

        consumer = createObjectUnderTest("json", true);
        consumer.onPartitionsAssigned(List.of(new TopicPartition(topic, testJsonPartition)));

        // Send one json record
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>(topic, testJsonPartition, 100L, testKey1, mapper.convertValue(testMap1, JsonNode.class));
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record1));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        // Send non-json record that results in deser exception
        RecordDeserializationException exc = new RecordDeserializationException(new TopicPartition(topic, testJsonPartition),
                101L, "Deserializedation exception", new AWSSchemaRegistryException("AWS glue parse exception"));
        when(kafkaConsumer.poll(any(Duration.class))).thenThrow(exc);
        consumer.consumeRecords();

        // Send one more json record
        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 102L, testKey2,
                mapper.convertValue(testMap2, JsonNode.class));
        records.clear();
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record2));
        consumerRecords = new ConsumerRecords(records);
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(consumerRecords);
        consumer.consumeRecords();

        Map.Entry<Collection<Record<Event>>, CheckpointState> bufferRecords = buffer.read(1000);
        ArrayList<Record<Event>> bufferedRecords = new ArrayList<>(bufferRecords.getKey());
        Assertions.assertEquals(2, bufferedRecords.size());
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 0);

        for (Record<Event> record : bufferedRecords) {
            Event event = record.getData();
            Map<String, Object> eventMap = event.toMap();
            String kafkaKey = event.get("kafka_key", String.class);
            assertTrue(kafkaKey.equals(testKey1) || kafkaKey.equals(testKey2));
            if (kafkaKey.equals(testKey1)) {
                testMap1.forEach((k, v) -> assertThat(eventMap, hasEntry(k, v)));
            }
            if (kafkaKey.equals(testKey2)) {
                testMap2.forEach((k, v) -> assertThat(eventMap, hasEntry(k, v)));
            }
            Assertions.assertNotNull(event.getMetadata().getExternalOriginationTime());
            Assertions.assertNotNull(event.getEventHandle().getExternalOriginationTime());
            event.getEventHandle().release(true);
        }
        // Wait for acknowledgement callback function to run
        try {
            Thread.sleep(100);
        } catch (Exception e){}

        consumer.processAcknowledgedOffsets();
        offsetsToCommit = consumer.getOffsetsToCommit();
        Assertions.assertEquals(offsetsToCommit.size(), 1);
        offsetsToCommit.forEach((topicPartition, offsetAndMetadata) -> {
            Assertions.assertEquals(topicPartition.partition(), testJsonPartition);
            Assertions.assertEquals(topicPartition.topic(), topic);
            Assertions.assertEquals(103L, offsetAndMetadata.offset());
        });
    }

    private ConsumerRecords createPlainTextRecords(String topic, final long startOffset) {
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic, testPartition, startOffset, testKey1, testValue1);
        ConsumerRecord<String, String> record2 = new ConsumerRecord<>(topic, testPartition, startOffset + 1, testKey2, testValue2);
        records.put(new TopicPartition(topic, testPartition), Arrays.asList(record1, record2));
        return new ConsumerRecords(records);
    }

    private ConsumerRecords createJsonRecords(String topic) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, JsonNode> record1 = new ConsumerRecord<>(topic, testJsonPartition, 100L, testKey1, mapper.convertValue(testMap1, JsonNode.class));
        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 101L, testKey2, mapper.convertValue(testMap2, JsonNode.class));
        records.put(new TopicPartition(topic, testJsonPartition), Arrays.asList(record1, record2));
        return new ConsumerRecords(records);
    }

    private ConsumerRecords createJsonOtelRecords(String topic) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic, testPartition, 100L, testKey1, jsonOtelTrace);
//        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 101L, testKey2, mapper.convertValue(testMap2, JsonNode.class));
        records.put(new TopicPartition(topic, testPartition), Arrays.asList(record1));
        return new ConsumerRecords(records);
    }

    private ConsumerRecords createJsonOtelRecordsWithErrors(String topic) throws Exception {
        final ObjectMapper mapper = new ObjectMapper();
        Map<TopicPartition, List<ConsumerRecord>> records = new HashMap<>();
        ConsumerRecord<String, String> record1 = new ConsumerRecord<>(topic, testPartition, 100L, testKey1, jsonOtelTraceWithError);
//        ConsumerRecord<String, JsonNode> record2 = new ConsumerRecord<>(topic, testJsonPartition, 101L, testKey2, mapper.convertValue(testMap2, JsonNode.class));
        records.put(new TopicPartition(topic, testPartition), Arrays.asList(record1));
        return new ConsumerRecords(records);
    }
}


