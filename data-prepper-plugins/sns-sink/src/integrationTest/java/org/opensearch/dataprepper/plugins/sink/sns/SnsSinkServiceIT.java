/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.sns;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.sns.dlq.DlqPushHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.sns.SnsSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_SNS_FAILED;
import static org.opensearch.dataprepper.plugins.sink.sns.SnsSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_SNS_SUCCESS;

public class SnsSinkServiceIT {

    private SnsClient snsClient;

    private PluginMetrics pluginMetrics;

    private PluginFactory pluginFactory;

    private PluginSetting pluginSetting;

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));


    private static final String SNS_SINK_CONFIG_YAML = "        topic_arn: {0}\n" +
            "        batch_size: 10\n" +
            "        aws:\n" +
            "          region: {1}\n" +
            "          sts_role_arn: {2}\n" +
            "        dlq_file: {3}\n" +
            "        codec:\n" +
            "          ndjson:\n" +
            "        max_retries: 5";

    private String standardTopic;

    private String fifoTopic;

    private String region;

    private String stsRoleArn;

    private String dlqFilePath;

    private Counter snsSinkObjectsEventsSucceeded;

    private Counter numberOfRecordsFailedCounter;

    private String standardSqsQueue;

    private SqsClient sqsClient;

    private String fifoSqsQueue;

    private DlqPushHandler dlqPushHandler;

    @BeforeEach
    public void setup() {
        this.standardTopic = System.getProperty("tests.sns.sink.standard.topic");
        this.fifoTopic = System.getProperty("tests.sns.sink.fifo.topic");
        this.region = System.getProperty("tests.sns.sink.region");
        this.stsRoleArn = System.getProperty("tests.sns.sink.sts.role.arn");
        this.dlqFilePath = System.getProperty("tests.sns.sink.dlq.file.path");
        this.standardSqsQueue = System.getProperty("tests.sns.sink.standard.sqs.queue.url");
        this.fifoSqsQueue = System.getProperty("tests.sns.sink.fifo.sqs.queue.url");

        this.dlqPushHandler = mock(DlqPushHandler.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.pluginFactory = mock(PluginFactory.class);
        this.pluginSetting = mock(PluginSetting.class);
        this.snsSinkObjectsEventsSucceeded = mock(Counter.class);
        this.numberOfRecordsFailedCounter = mock(Counter.class);
        this.snsClient = SnsClient.builder()
                .region(Region.of(region))
                .build();
        this.sqsClient = SqsClient.builder().region(Region.of(region)).build();
        when(pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_SNS_SUCCESS)).thenReturn(snsSinkObjectsEventsSucceeded);
        when(pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_SNS_FAILED)).thenReturn(numberOfRecordsFailedCounter);
    }

    private Collection<Record<Event>> setEventQueue(final int records) {
        final Collection<Record<Event>> jsonObjects = new LinkedList<>();
        for (int i = 0; i < records; i++)
            jsonObjects.add(createRecord());
        return jsonObjects;
    }

    private static Record<Event> createRecord() {
        final JacksonEvent event = JacksonLog.builder().withData("[{\"name\":\"test\"}]").build();
        event.setEventHandle(mock(EventHandle.class));
        return new Record<>(event);
    }

    public SnsSinkService createObjectUnderTest(final String topicName) throws JsonProcessingException {
        String[] values = { topicName,region,stsRoleArn,dlqFilePath };
        final String configYaml = MessageFormat.format(SNS_SINK_CONFIG_YAML, values);
        final SnsSinkConfig snsSinkConfig = objectMapper.readValue(configYaml, SnsSinkConfig.class);
        return new SnsSinkService(snsSinkConfig,snsClient,pluginMetrics,pluginFactory,pluginSetting,mock(ExpressionEvaluator.class));
    }

    @ParameterizedTest
    @ValueSource(ints = {5,9,10})
    public void sns_sink_service_test_with_standard_queue_with_multiple_records(final int recordCount) throws JsonProcessingException, InterruptedException {
        final SnsSinkService objectUnderTest = createObjectUnderTest(standardTopic);
        final Collection<Record<Event>> records = setEventQueue(recordCount);
        final List<String> inputRecords = records.stream().map(Record::getData).map(Event::toJsonString).collect(Collectors.toList());
        objectUnderTest.output(records);
        Thread.sleep(Duration.ofSeconds(10).toMillis());
        List<String> topicData = readMessagesFromSNSTopicQueue(inputRecords,standardSqsQueue);
        assertThat(inputRecords, is(topicData));
        assertThat(inputRecords.size(), equalTo(topicData.size()));
        verify(snsSinkObjectsEventsSucceeded).increment(recordCount);
    }

    @Test
    public void sns_sink_service_test_with_standard_queue_with_multiple_batch() throws JsonProcessingException, InterruptedException {
        final SnsSinkService objectUnderTest = createObjectUnderTest(standardTopic);
            final Collection<Record<Event>> records = setEventQueue(11);
        final List<String> inputRecords = records.stream().map(Record::getData).map(Event::toJsonString).collect(Collectors.toList());
        objectUnderTest.output(records);
        Thread.sleep(Duration.ofSeconds(10).toMillis());
        List<String> topicData = readMessagesFromSNSTopicQueue(inputRecords,standardSqsQueue);
        assertThat(inputRecords, is(topicData));
        assertThat(inputRecords.size(), equalTo(topicData.size()));
        verify(snsSinkObjectsEventsSucceeded,times(2)).increment(anyDouble());
    }

    private List<String> readMessagesFromSNSTopicQueue(List<String> inputRecords, final String sqsQueue) {
        final List<Message> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 60000;
        do {
            messages.addAll(sqsClient.receiveMessage(builder -> builder.queueUrl(sqsQueue)).messages());
            if(messages.size() >= inputRecords.size()){
                break;
            }
        } while (System.currentTimeMillis() < endTime);

        List<String> topicData = messages.stream().map(Message::body).map(obj-> {
            try {
                Map<String,String> map = objectMapper.readValue(obj,Map.class);
                return map.get("Message");
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        return topicData;
    }

    private void deleteSqsMessages(String sqsQueue, List<Message> messages) throws InterruptedException {
        for (Message message : messages) {
            sqsClient.deleteMessage(builder -> builder.queueUrl(sqsQueue).receiptHandle(message.receiptHandle()));
            Thread.sleep(Duration.ofSeconds(2).toMillis());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10})
    public void sns_sink_service_test_with_fifo_queue_with_multiple_records(final int recordCount) throws JsonProcessingException, InterruptedException {
        final SnsSinkService objectUnderTest = createObjectUnderTest(fifoTopic);
        final Collection<Record<Event>> records = setEventQueue(recordCount);
        final List<String> inputRecords = records.stream().map(Record::getData).map(Event::toJsonString).collect(Collectors.toList());
        objectUnderTest.output(records);
        Thread.sleep(Duration.ofSeconds(10).toMillis());
        List<String> topicData = readMessagesFromSNSTopicQueue(inputRecords,fifoSqsQueue);
        assertThat(inputRecords, is(topicData));
        assertThat(inputRecords.size(), equalTo(topicData.size()));
        verify(snsSinkObjectsEventsSucceeded).increment(recordCount);
    }



    @ParameterizedTest
    @ValueSource(ints = {1,5,9})
    public void sns_sink_service_test_fail_to_push(final int recordCount) throws IOException, InterruptedException {
        final ObjectMapper mapper = new ObjectMapper();
        final String topic = "test";
        Files.deleteIfExists(Path.of(dlqFilePath));
        final SnsSinkService objectUnderTest = createObjectUnderTest(topic);
        final Collection<Record<Event>> records = setEventQueue(recordCount);
        objectUnderTest.output(records);
        Thread.sleep(Duration.ofSeconds(10).toMillis());
        verify(numberOfRecordsFailedCounter).increment(recordCount);
        final Map<String,String> map = mapper.readValue(new String(Files.readAllBytes(Path.of(dlqFilePath))).replaceAll("(\\r|\\n)", ""), Map.class);
        assertThat(map.get("topic"),equalTo(topic));
    }
}