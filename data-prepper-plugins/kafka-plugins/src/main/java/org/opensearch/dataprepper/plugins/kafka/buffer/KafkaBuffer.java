/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.AbstractBuffer;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.kafka.admin.KafkaAdminAccessor;
import org.opensearch.dataprepper.plugins.kafka.buffer.serialization.BufferSerializationFactory;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.CommonSerializationFactory;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumerFactory;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducer;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@DataPrepperPlugin(name = "kafka", pluginType = Buffer.class, pluginConfigurationType = KafkaBufferConfig.class)
public class KafkaBuffer extends AbstractBuffer<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBuffer.class);
    static final long EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT = 30L;
    public static final int INNER_BUFFER_CAPACITY = 1000000;
    public static final int INNER_BUFFER_BATCH_SIZE = 250000;
    static final String WRITE = "Write";
    static final String READ = "Read";
    private final KafkaCustomProducer producer;
    private final KafkaAdminAccessor kafkaAdminAccessor;
    private final AbstractBuffer<Record<Event>> innerBuffer;
    private final ExecutorService executorService;
    private final Duration drainTimeout;
    private AtomicBoolean shutdownInProgress;
    private ByteDecoder byteDecoder;

    @DataPrepperPluginConstructor
    public KafkaBuffer(final PluginSetting pluginSetting, final KafkaBufferConfig kafkaBufferConfig, final PluginFactory pluginFactory,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final ByteDecoder byteDecoder, final AwsCredentialsSupplier awsCredentialsSupplier,
                       final CircuitBreaker circuitBreaker) {
        super(kafkaBufferConfig.getCustomMetricPrefix().orElse(pluginSetting.getName()), pluginSetting.getPipelineName());
        final SerializationFactory serializationFactory = new BufferSerializationFactory(new CommonSerializationFactory());
        final KafkaCustomProducerFactory kafkaCustomProducerFactory = new KafkaCustomProducerFactory(serializationFactory, awsCredentialsSupplier);
        this.byteDecoder = byteDecoder;
        final String metricPrefixName = kafkaBufferConfig.getCustomMetricPrefix().orElse(pluginSetting.getName());
        final PluginMetrics producerMetrics = PluginMetrics.fromNames(metricPrefixName + WRITE, pluginSetting.getPipelineName());
        producer = kafkaCustomProducerFactory.createProducer(kafkaBufferConfig, pluginFactory, pluginSetting,  null, null, producerMetrics, false);
        final KafkaCustomConsumerFactory kafkaCustomConsumerFactory = new KafkaCustomConsumerFactory(serializationFactory, awsCredentialsSupplier);
        innerBuffer = new BlockingBuffer<>(INNER_BUFFER_CAPACITY, INNER_BUFFER_BATCH_SIZE, pluginSetting.getPipelineName());
        this.shutdownInProgress = new AtomicBoolean(false);
        final PluginMetrics consumerMetrics = PluginMetrics.fromNames(metricPrefixName + READ, pluginSetting.getPipelineName());
        final List<KafkaCustomConsumer> consumers = kafkaCustomConsumerFactory.createConsumersForTopic(kafkaBufferConfig, kafkaBufferConfig.getTopic(),
            innerBuffer, consumerMetrics, acknowledgementSetManager, byteDecoder, shutdownInProgress, false, circuitBreaker);
        this.kafkaAdminAccessor = new KafkaAdminAccessor(kafkaBufferConfig, List.of(kafkaBufferConfig.getTopic().getGroupId()));
        this.executorService = Executors.newFixedThreadPool(consumers.size());
        consumers.forEach(this.executorService::submit);

        this.drainTimeout = kafkaBufferConfig.getDrainTimeout();
    }

    @Override
    public void writeBytes(final byte[] bytes, final String key, int timeoutInMillis) throws Exception {
        try {
            producer.produceRawData(bytes, key);
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doWrite(Record<Event> record, int timeoutInMillis) throws TimeoutException {
        try {
            producer.produceRecords(record);
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isByteBuffer() {
        return true;
    }

    @Override
    public void doWriteAll(Collection<Record<Event>> records, int timeoutInMillis) throws Exception {
        for ( Record<Event> record: records ) {
            doWrite(record, timeoutInMillis);
        }
    }

    @Override
    public Map.Entry<Collection<Record<Event>>, CheckpointState> doRead(int timeoutInMillis) {
        return innerBuffer.doRead(timeoutInMillis);
    }

    @Override
    public void postProcess(final Long recordsInBuffer) {
        innerBuffer.postProcess(recordsInBuffer);
    }

    @Override
    public void doCheckpoint(CheckpointState checkpointState) {
        innerBuffer.doCheckpoint(checkpointState);
    }

    @Override
    public boolean isEmpty() {
        return kafkaAdminAccessor.areTopicsEmpty() && innerBuffer.isEmpty();
    }

    @Override
    public Duration getDrainTimeout() {
        return drainTimeout;
    }

    @Override
    public boolean isWrittenOffHeapOnly() {
        return true;
    }

    @Override
    public void shutdown() {
        shutdownInProgress.set(true);
        executorService.shutdown();

        try {
            if (executorService.awaitTermination(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
                LOG.info("Successfully waited for consumer task to terminate");
            } else {
                LOG.warn("Consumer task did not terminate in time, forcing termination");
                executorService.shutdownNow();
            }
        } catch (final InterruptedException e) {
            LOG.error("Interrupted while waiting for consumer task to terminate", e);
            executorService.shutdownNow();
        }

        innerBuffer.shutdown();
    }
}
