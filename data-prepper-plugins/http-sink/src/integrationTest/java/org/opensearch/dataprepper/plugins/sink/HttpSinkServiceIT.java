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
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import org.opensearch.dataprepper.plugins.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.service.HttpSinkService;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.service.HttpSinkService.HTTP_SINK_RECORDS_SUCCESS_COUNTER;

public class HttpSinkServiceIT {

    private String urlString;

    OutputCodec codec;

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private String config =
            "        url: {0}\n" +
            "        http_method: POST\n" +
            "        auth_type: {1}\n" +
            "        codec:\n" +
            "          ndjson:\n" +
            "        ssl: false\n" +
            "        aws:\n" +
            "          region: {2}\n" +
            "          sts_role_arn: {3}\n" +
            "        threshold:\n" +
            "          event_count: 1";

    private HttpSinkConfiguration httpSinkConfiguration;

    private BufferFactory bufferFactory;

    private DlqPushHandler dlqPushHandler;

    private PluginMetrics pluginMetrics;


    @BeforeEach
    void setUp() throws JsonProcessingException{
        this.urlString = System.getProperty("tests.http.sink.http.endpoint");
        String[] values = { urlString,"unauthenticated","ap-south-1","arn:aws:iam::524239988944:role/app-test" };
        final String configYaml = MessageFormat.format(config, values);
        this.httpSinkConfiguration = objectMapper.readValue(configYaml, HttpSinkConfiguration.class);
    }

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private Counter httpSinkRecordsSuccessCounter;

    public HttpSinkService createHttpSinkServiceUnderTest() throws NoSuchFieldException, IllegalAccessException {
        this.pipelineDescription = mock(PipelineDescription.class);
        this.pluginFactory = mock(PluginFactory.class);
        this.httpSinkRecordsSuccessCounter = mock(Counter.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        when(pluginMetrics.counter(HTTP_SINK_RECORDS_SUCCESS_COUNTER)).thenReturn(httpSinkRecordsSuccessCounter);
        when(pipelineDescription.getPipelineName()).thenReturn("http-plugin");
        ReflectivelySetField.setField(ThresholdOptions.class,httpSinkConfiguration.getThresholdOptions(),"eventCollectTimeOut", Duration.ofNanos(1));

        final PluginModel codecConfiguration = httpSinkConfiguration.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());

        this.bufferFactory = new InMemoryBufferFactory();
        this.dlqPushHandler = new DlqPushHandler(httpSinkConfiguration.getDlqFile(), pluginFactory,
                "bucket",
                "arn", "region",
                "keypath");

        HttpClientBuilder httpClientBuilder = HttpClients.custom();

        return new HttpSinkService(
                httpSinkConfiguration,
                bufferFactory,
                dlqPushHandler,
                codecPluginSettings,
                null,
                httpClientBuilder,
                pluginMetrics,
                null);
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

    @Test
    public void http_endpoint_test_with_single_record() throws NoSuchFieldException, IllegalAccessException {
        final HttpSinkService httpSinkService = createHttpSinkServiceUnderTest();
        final Collection<Record<Event>> records = setEventQueue(1);
        httpSinkService.output(records);
        verify(httpSinkRecordsSuccessCounter).increment(records.size());
    }

}
