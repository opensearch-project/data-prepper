package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaBufferConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaBufferIT {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBufferIT.class);
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private KafkaBufferConfig kafkaBufferConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private PluginMetrics pluginMetrics;

    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(pluginSetting.getPipelineName()).thenReturn(UUID.randomUUID().toString());

        MessageFormat messageFormat = MessageFormat.JSON;

        String topicName = "buffer-" + RandomStringUtils.randomAlphabetic(5);
        TopicConfig topicConfig = mock(TopicConfig.class);
        when(topicConfig.getName()).thenReturn(topicName);
        when(topicConfig.getGroupId()).thenReturn("buffergroup-" + RandomStringUtils.randomAlphabetic(6));
        when(topicConfig.isCreate()).thenReturn(true);
        when(topicConfig.getSerdeFormat()).thenReturn(messageFormat);
        when(topicConfig.getWorkers()).thenReturn(1);
        when(topicConfig.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(topicConfig.getConsumerMaxPollRecords()).thenReturn(1);
        when(topicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(topicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(3));
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(topicConfig.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(1));
        when(kafkaBufferConfig.getTopic()).thenReturn(topicConfig);
        when(kafkaBufferConfig.getSerdeFormat()).thenReturn(messageFormat.toString());
        when(kafkaBufferConfig.getPartitionKey()).thenReturn(UUID.randomUUID().toString());

        EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);

        String bootstrapServers = System.getProperty("tests.kafka.bootstrap_servers");

        LOG.info("Using Kafka bootstrap servers: {}", bootstrapServers);

        when(kafkaBufferConfig.getBootstrapServers()).thenReturn(Collections.singletonList(bootstrapServers));
        when(kafkaBufferConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
    }

    private KafkaBuffer<Record<Event>> createObjectUnderTest() {
        return new KafkaBuffer<>(pluginSetting, kafkaBufferConfig, pluginFactory, acknowledgementSetManager, pluginMetrics);
    }

    @Test
    void write_and_read() throws TimeoutException {
        KafkaBuffer<Record<Event>> objectUnderTest = createObjectUnderTest();

        Record<Event> record = createRecord();
        objectUnderTest.write(record, 1_000);

        Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = objectUnderTest.read(10_000);

        assertThat(readResult, notNullValue());
        assertThat(readResult.getKey(), notNullValue());
        assertThat(readResult.getKey().size(), equalTo(1));

        Record<Event> onlyResult = readResult.getKey().stream().iterator().next();

        assertThat(onlyResult, notNullValue());
        assertThat(onlyResult.getData(), notNullValue());
        // TODO: The metadata is not included. It needs to be included in the Buffer, though not in the Sink. This may be something we make configurable in the consumer/producer - whether to serialize the metadata or not.
        //assertThat(onlyResult.getData().getMetadata(), equalTo(record.getData().getMetadata()));
        assertThat(onlyResult.getData().toMap(), equalTo(record.getData().toMap()));
    }

    private Record<Event> createRecord() {
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        return new Record<>(event);
    }
}
