package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.AbstractBuffer;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaBufferConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaCustomConsumerFactory;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducer;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaCustomProducerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@DataPrepperPlugin(name = "kafka", pluginType = Buffer.class, pluginConfigurationType = KafkaBufferConfig.class)
public class KafkaBuffer<T extends Record<?>> extends AbstractBuffer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBuffer.class);
    public static final int INNER_BUFFER_CAPACITY = 1000000;
    public static final int INNER_BUFFER_BATCH_SIZE = 250000;
    private final KafkaCustomProducer producer;
    private final AbstractBuffer innerBuffer;
    private final ExecutorService executorService;

    @DataPrepperPluginConstructor
    public KafkaBuffer(final PluginSetting pluginSetting, final KafkaBufferConfig kafkaBufferConfig, final PluginFactory pluginFactory,
                       final AcknowledgementSetManager acknowledgementSetManager, final PluginMetrics pluginMetrics){
        super(pluginSetting);
        final KafkaCustomProducerFactory kafkaCustomProducerFactory = new KafkaCustomProducerFactory();
        producer = kafkaCustomProducerFactory.createProducer(kafkaBufferConfig, pluginFactory, pluginSetting,  null, null);
        final KafkaCustomConsumerFactory kafkaCustomConsumerFactory = new KafkaCustomConsumerFactory();
        innerBuffer = new BlockingBuffer<>(INNER_BUFFER_CAPACITY, INNER_BUFFER_BATCH_SIZE, pluginSetting.getPipelineName());
        final List<KafkaCustomConsumer> consumers = kafkaCustomConsumerFactory.createConsumersForTopic(kafkaBufferConfig, kafkaBufferConfig.getTopic(),
            innerBuffer, pluginMetrics, acknowledgementSetManager, new AtomicBoolean(false));
        this.executorService = Executors.newFixedThreadPool(consumers.size());
        consumers.forEach(this.executorService::submit);
    }

    @Override
    public void doWrite(T record, int timeoutInMillis) throws TimeoutException {
        try {
            producer.produceRecords(record);
        } catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doWriteAll(Collection<T> records, int timeoutInMillis) throws Exception {
        for ( T record: records ) {
            doWrite(record, timeoutInMillis);
        }
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> doRead(int timeoutInMillis) {
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
        // TODO: check Kafka topic is empty as well.
        return innerBuffer.isEmpty();
    }
}
