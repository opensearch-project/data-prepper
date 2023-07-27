/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.SNSSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_SNS_FAILED;
import static org.opensearch.dataprepper.plugins.sink.SNSSinkService.NUMBER_OF_RECORDS_FLUSHED_TO_SNS_SUCCESS;

public class SNSSinkServiceIT{

    private SnsClient snsClient;

    private PluginMetrics pluginMetrics;

    private PluginFactory pluginFactory;

    private PluginSetting pluginSetting;

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));


    private static final String SNS_SINK_CONFIG_YAML = "        topic: {0}\n" +
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


    @BeforeEach
    public void setup() {
        this.standardTopic = System.getProperty("tests.sns.sink.standard.topic");
        this.fifoTopic = System.getProperty("tests.sns.sink.fifo.topic");
        this.region = System.getProperty("tests.sns.sink.region");
        this.stsRoleArn = System.getProperty("tests.sns.sink.sts.role.arn");
        this.dlqFilePath = System.getProperty("tests.sns.sink.dlq.file.path");

        this.pluginMetrics = mock(PluginMetrics.class);
        this.pluginFactory = mock(PluginFactory.class);
        this.pluginSetting = mock(PluginSetting.class);
        this.snsSinkObjectsEventsSucceeded = mock(Counter.class);
        this.numberOfRecordsFailedCounter = mock(Counter.class);
        this.snsClient = SnsClient.builder()
                .region(Region.of(region))
                .build();
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
        final JacksonEvent event = JacksonLog.builder().withData("[{\"name\":\""+ UUID.randomUUID() +"\"}]").build();
        event.setEventHandle(mock(EventHandle.class));
        return new Record<>(event);
    }

    public SNSSinkService createObjectUnderTest(final String topicName) throws JsonProcessingException {
        String[] values = { topicName,region,stsRoleArn,dlqFilePath };
        final String configYaml = MessageFormat.format(SNS_SINK_CONFIG_YAML, values);
        final SNSSinkConfig snsSinkConfig = objectMapper.readValue(configYaml, SNSSinkConfig.class);
        return new SNSSinkService(snsSinkConfig,snsClient,pluginMetrics,pluginFactory,pluginSetting);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10})
    public void sns_sink_service_test_with_standard_queue_with_multiple_records(final int recordCount) throws JsonProcessingException {
        final SNSSinkService objectUnderTest = createObjectUnderTest(standardTopic);
        final Collection<Record<Event>> records = setEventQueue(recordCount);
        objectUnderTest.output(records);
        verify(snsSinkObjectsEventsSucceeded).increment(recordCount);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5, 10})
    public void sns_sink_service_test_with_fifo_queue_with_multiple_records(final int recordCount) throws JsonProcessingException {
        final SNSSinkService objectUnderTest = createObjectUnderTest(fifoTopic);
        final Collection<Record<Event>> records = setEventQueue(recordCount);
        objectUnderTest.output(records);
        verify(snsSinkObjectsEventsSucceeded).increment(recordCount);
    }

    @ParameterizedTest
    @ValueSource(ints = {1,5,9})
    public void sns_sink_service_test_fail_to_push(final int recordCount) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final String topic = "test";
        Files.deleteIfExists(Path.of(dlqFilePath));
        final SNSSinkService objectUnderTest = createObjectUnderTest(topic);
        final Collection<Record<Event>> records = setEventQueue(recordCount);
        objectUnderTest.output(records);
        verify(numberOfRecordsFailedCounter).increment(recordCount);
        final Map<String,String> map = mapper.readValue(new String(Files.readAllBytes(Path.of(dlqFilePath))).replaceAll("(\\r|\\n)", ""), Map.class);
        assertThat(map.get("topic"),equalTo(topic));
    }
}