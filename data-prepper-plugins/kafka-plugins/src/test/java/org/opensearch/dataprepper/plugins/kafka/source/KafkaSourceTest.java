/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.time.Duration;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceTest {
    private KafkaSource kafkaSource;

    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SchemaConfig schemaConfig;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private TopicConfig topicConfig;
    @Mock
    private PipelineDescription pipelineDescription;
    @Mock
    PlainTextAuthConfig plainTextAuthConfig;
    @Mock
    TopicConfig topic1, topic2;
    @Mock
    private Buffer<Record<Event>> buffer;

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "my-topic";
    private static final String TEST_GROUP_ID = "testGroupId";


    public KafkaSource createObjectUnderTest() {
        return new KafkaSource(sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription);
    }

    @BeforeEach
    void setUp() throws Exception {
        sourceConfig = mock(KafkaSourceConfig.class);
        pipelineDescription = mock(PipelineDescription.class);
        pluginMetrics = mock(PluginMetrics.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        when(topic1.getName()).thenReturn("topic1");
        when(topic2.getName()).thenReturn("topic2");
        when(topic1.getWorkers()).thenReturn(2);
        when(topic2.getWorkers()).thenReturn(3);
        when(topic1.getAutoCommitInterval()).thenReturn(Duration.ofSeconds(1));
        when(topic2.getAutoCommitInterval()).thenReturn(Duration.ofSeconds(1));
        when(topic1.getAutoOffsetReset()).thenReturn("earliest");
        when(topic2.getAutoOffsetReset()).thenReturn("earliest");
        when(topic1.getConsumerMaxPollRecords()).thenReturn(1);
        when(topic2.getConsumerMaxPollRecords()).thenReturn(1);
        when(topic1.getGroupId()).thenReturn(TEST_GROUP_ID);
        when(topic2.getGroupId()).thenReturn(TEST_GROUP_ID);
        when(topic1.getAutoCommit()).thenReturn(false);
        when(topic2.getAutoCommit()).thenReturn(false);
        when(topic1.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(10));
        when(topic2.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(10));
        when(sourceConfig.getBootStrapServers()).thenReturn(List.of("http://localhost:1234"));
        when(sourceConfig.getTopics()).thenReturn(Arrays.asList(topic1, topic2));
    }

    @Test
    void test_kafkaSource_start_stop() {
        kafkaSource = createObjectUnderTest();
        kafkaSource.start(buffer);
        try {
            Thread.sleep(10);
        } catch (Exception e){}
        kafkaSource.stop();
    }

    @Test
    void test_kafkaSource_start_execution_catch_block() {
        kafkaSource = createObjectUnderTest();
        Assertions.assertThrows(Exception.class, () -> kafkaSource.start(null));
    }
}
