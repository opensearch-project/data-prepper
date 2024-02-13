/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

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
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.kafka.admin.KafkaAdminAccessor;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumerFactory;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducer;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducerFactory;
import org.opensearch.dataprepper.plugins.kafka.producer.ProducerWorker;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBuffer.EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaBufferTest {

    private static final String TEST_GROUP_ID = "testGroupId";

    private KafkaBuffer kafkaBuffer;
    ExecutorService executorService;
    @Mock
    private KafkaBufferConfig bufferConfig;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    BufferTopicConfig topic1;
    @Mock
    AuthConfig authConfig;
    @Mock
    AuthConfig.SaslAuthConfig saslAuthConfig;
    @Mock
    PlainTextAuthConfig plainTextAuthConfig;

    @Mock
    private EncryptionConfig encryptionConfig;

    @Mock
    FutureTask futureTask;

    @Mock
    KafkaCustomProducerFactory producerFactory;

    @Mock
    KafkaCustomProducer<Event> producer;

    @Mock
    private KafkaCustomConsumerFactory consumerFactory;

    @Mock
    private KafkaCustomConsumer consumer;

    @Mock
    private KafkaAdminAccessor kafkaAdminAccessor;

    @Mock
    BlockingBuffer<Record<Event>>  blockingBuffer;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private CircuitBreaker circuitBreaker;

    public KafkaBuffer createObjectUnderTest() {
        return createObjectUnderTest(List.of(consumer));
    }

    public KafkaBuffer createObjectUnderTest(final List<KafkaCustomConsumer> consumers) {
        try (
            final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class);
            final MockedConstruction<KafkaCustomProducerFactory> producerFactoryMock =
                mockConstruction(KafkaCustomProducerFactory.class, (mock, context) -> {
                producerFactory = mock;
                when(producerFactory.createProducer(any(), isNull(), isNull(), any(), any(), anyBoolean())).thenReturn(producer);
            });
            final MockedConstruction<KafkaCustomConsumerFactory> consumerFactoryMock =
                mockConstruction(KafkaCustomConsumerFactory.class, (mock, context) -> {
                consumerFactory = mock;
                when(consumerFactory.createConsumersForTopic(any(), any(), any(), any(), any(), any(), any(), anyBoolean(), any())).thenReturn(consumers);
            });
            final MockedConstruction<KafkaAdminAccessor> adminAccessorMock =
                mockConstruction(KafkaAdminAccessor.class, (mock, context) -> kafkaAdminAccessor = mock);
            final MockedConstruction<BlockingBuffer> blockingBufferMock =
                 mockConstruction(BlockingBuffer.class, (mock, context) -> {
                     blockingBuffer = mock;
                 })) {

            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executorService);
            return new KafkaBuffer(pluginSetting, bufferConfig, acknowledgementSetManager, null, awsCredentialsSupplier, circuitBreaker);
        }
    }



    @BeforeEach
    void setUp() {
        when(pluginSetting.getPipelineName()).thenReturn("pipeline");
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        when(topic1.getName()).thenReturn("topic1");
        when(topic1.isCreateTopic()).thenReturn(true);

        when(topic1.getWorkers()).thenReturn(2);
        when(topic1.getCommitInterval()).thenReturn(Duration.ofSeconds(1));
        when(topic1.getAutoOffsetReset()).thenReturn("earliest");
        when(topic1.getConsumerMaxPollRecords()).thenReturn(1);
        when(topic1.getGroupId()).thenReturn(TEST_GROUP_ID);
        when(topic1.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(topic1.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        when(topic1.getAutoCommit()).thenReturn(false);
        when(topic1.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topic1.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(10));
        when(topic1.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));

        when(bufferConfig.getBootstrapServers()).thenReturn(Collections.singletonList("http://localhost:1234"));
        when(bufferConfig.getTopic()).thenReturn(topic1);

        when(bufferConfig.getSchemaConfig()).thenReturn(null);
        when(bufferConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
        when(bufferConfig.getSerdeFormat()).thenReturn("plaintext");

        when(bufferConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        when(plainTextAuthConfig.getUsername()).thenReturn("username");
        when(plainTextAuthConfig.getPassword()).thenReturn("password");

        executorService = mock(ExecutorService.class);
        when(executorService.submit(any(ProducerWorker.class))).thenReturn(futureTask);

    }

    @Test
    void test_kafkaBuffer_basicFunctionality() throws TimeoutException, Exception {
        kafkaBuffer = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaBuffer));

        Record<Event> record = new Record<Event>(JacksonEvent.fromMessage(UUID.randomUUID().toString()));
        kafkaBuffer.doWrite(record, 10000);
        kafkaBuffer.doRead(10000);
        verify(producer).produceRecords(record);
        verify(blockingBuffer).read(anyInt());
    }

    @Test
    void test_kafkaBuffer_producerThrows() throws TimeoutException, Exception {

        kafkaBuffer = createObjectUnderTest();
        Record<Event> record = new Record<Event>(JacksonEvent.fromMessage(UUID.randomUUID().toString()));
        doThrow(new RuntimeException("Producer Error"))
            .when(producer).produceRecords(record);

        assertThrows(RuntimeException.class, () -> kafkaBuffer.doWrite(record, 10000));
    }

    @Test
    void test_kafkaBuffer_doWriteAll() throws Exception {
        kafkaBuffer = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaBuffer));

        Record<Event> record = new Record<Event>(JacksonEvent.fromMessage(UUID.randomUUID().toString()));
        Record<Event> record2 = new Record<Event>(JacksonEvent.fromMessage(UUID.randomUUID().toString()));

        kafkaBuffer.doWriteAll(Arrays.asList(record,record2), 10000);
        verify(producer).produceRecords(record);
        verify(producer).produceRecords(record2);
    }

    @Test
    void test_kafkaBuffer_isEmpty_True() {
        kafkaBuffer = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaBuffer));
        when(blockingBuffer.isEmpty()).thenReturn(true);
        when(kafkaAdminAccessor.areTopicsEmpty()).thenReturn(true);

        final boolean result = kafkaBuffer.isEmpty();
        assertThat(result, equalTo(true));

        verify(blockingBuffer).isEmpty();
        verify(kafkaAdminAccessor).areTopicsEmpty();
    }

    @Test
    void test_kafkaBuffer_isEmpty_BufferNotEmpty() {
        kafkaBuffer = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaBuffer));
        when(blockingBuffer.isEmpty()).thenReturn(false);
        when(kafkaAdminAccessor.areTopicsEmpty()).thenReturn(true);

        final boolean result = kafkaBuffer.isEmpty();
        assertThat(result, equalTo(false));

        verify(blockingBuffer).isEmpty();
        verify(kafkaAdminAccessor).areTopicsEmpty();
    }

    @Test
    void test_kafkaBuffer_isEmpty_TopicNotEmpty() {
        kafkaBuffer = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaBuffer));
        when(blockingBuffer.isEmpty()).thenReturn(true);
        when(kafkaAdminAccessor.areTopicsEmpty()).thenReturn(false);

        final boolean result = kafkaBuffer.isEmpty();
        assertThat(result, equalTo(false));

        verifyNoInteractions(blockingBuffer);
        verify(kafkaAdminAccessor).areTopicsEmpty();
    }

    @Test
    void test_kafkaBuffer_isEmpty_MultipleTopics_AllNotEmpty() {
        kafkaBuffer = createObjectUnderTest(List.of(consumer, consumer));
        assertTrue(Objects.nonNull(kafkaBuffer));
        when(blockingBuffer.isEmpty()).thenReturn(true);
        when(kafkaAdminAccessor.areTopicsEmpty()).thenReturn(false).thenReturn(false);

        final boolean result = kafkaBuffer.isEmpty();
        assertThat(result, equalTo(false));

        verifyNoInteractions(blockingBuffer);
        verify(kafkaAdminAccessor).areTopicsEmpty();
    }

    @Test
    void test_kafkaBuffer_doCheckpoint() {
        kafkaBuffer = createObjectUnderTest();
        kafkaBuffer.doCheckpoint(mock(CheckpointState.class));
        verify(blockingBuffer).doCheckpoint(any());
    }

    @Test
    void test_kafkaBuffer_postProcess() {
        kafkaBuffer = createObjectUnderTest();
        kafkaBuffer.postProcess(0L);
        verify(blockingBuffer).postProcess(0L);
    }


    @Test
    void test_kafkaBuffer_getDrainTimeout() {
        final Duration duration = Duration.ofMillis(new Random().nextLong());
        when(bufferConfig.getDrainTimeout()).thenReturn(duration);
        kafkaBuffer = createObjectUnderTest();

        final Duration result = kafkaBuffer.getDrainTimeout();
        assertThat(result, equalTo(duration));

        verify(bufferConfig).getDrainTimeout();
    }

    @Test
    void isWrittenOffHeapOnly_returns_true() {
        assertThat(createObjectUnderTest().isWrittenOffHeapOnly(),
                equalTo(true));
    }

    @Test
    public void testShutdown_Successful() throws InterruptedException {
        kafkaBuffer = createObjectUnderTest();
        lenient().when(executorService.awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.SECONDS))).thenReturn(true);

        kafkaBuffer.shutdown();
        verify(executorService).shutdown();
        verify(executorService).awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.SECONDS));
    }

    @Test
    public void testShutdown_Timeout() throws InterruptedException {
        kafkaBuffer = createObjectUnderTest();
        lenient().when(executorService.awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.SECONDS))).thenReturn(false);

        kafkaBuffer.shutdown();
        verify(executorService).shutdown();
        verify(executorService).awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.SECONDS));
        verify(executorService).shutdownNow();
    }

    @Test
    public void testShutdown_InterruptedException() throws InterruptedException {
        kafkaBuffer = createObjectUnderTest();
        lenient().when(executorService.awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.SECONDS)))
                .thenThrow(new InterruptedException());

        kafkaBuffer.shutdown();
        verify(executorService).shutdown();
        verify(executorService).awaitTermination(eq(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT), eq(TimeUnit.SECONDS));
        verify(executorService).shutdownNow();
    }
}
