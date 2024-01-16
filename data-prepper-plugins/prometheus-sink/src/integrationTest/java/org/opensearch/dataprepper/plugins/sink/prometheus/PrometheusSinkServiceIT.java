/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.sink.prometheus.service.PrometheusSinkService;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrometheusSinkServiceIT {

    private String urlString;

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    private String config =
            "        url: {0}\n" +
                    "        http_method: POST\n" +
                    "        auth_type: {1}\n" +
                    "        insecure_skip_verify: true\n";

    private PrometheusSinkConfiguration prometheusSinkConfiguration;

    private DlqPushHandler dlqPushHandler;

    private PluginMetrics pluginMetrics;

    private PluginSetting pluginSetting;

    @BeforeEach
    void setUp() throws JsonProcessingException{
        this.urlString = System.getProperty("tests.prometheus.sink.http.endpoint");
        String[] values = { urlString,"unauthenticated"};
        final String configYaml = MessageFormat.format(config, values);
        this.prometheusSinkConfiguration = objectMapper.readValue(configYaml, PrometheusSinkConfiguration.class);
    }

    @Mock
    private PipelineDescription pipelineDescription;

    private PluginFactory pluginFactory;


    public PrometheusSinkService createPrometheusSinkServiceUnderTest() throws NoSuchFieldException, IllegalAccessException {
        this.pipelineDescription = mock(PipelineDescription.class);
        this.pluginFactory = mock(PluginFactory.class);
        this.pluginMetrics = mock(PluginMetrics.class);
        this.pluginSetting = mock(PluginSetting.class);

        when(pipelineDescription.getPipelineName()).thenReturn("prometheus-plugin");

        this.dlqPushHandler = new DlqPushHandler(prometheusSinkConfiguration.getDlqFile(), pluginFactory,
                "bucket",
                "arn", "region",
                "keypath", pluginMetrics);

        HttpClientBuilder httpClientBuilder = HttpClients.custom();

        return new PrometheusSinkService(
                prometheusSinkConfiguration,
                dlqPushHandler,
                httpClientBuilder,
                pluginMetrics,
                pluginSetting);
    }

    private Collection<Record<Event>> setEventQueue(final int records) {
        final Collection<Record<Event>> jsonObjects = new LinkedList<>();
        for (int i = 0; i < records; i++)
            jsonObjects.add(createRecord());
        return jsonObjects;
    }

    private static Record<Event> createRecord() {
        EventMetadata eventMetadata = new DefaultEventMetadata.Builder().withEventType("METRIC").build();
        Map<String,Object> attributeMap = new HashMap<>();
        attributeMap.put("MyLableKey","MyLableValue");
        final JacksonEvent event = JacksonGauge.builder()
                .withName("prometheus")
                .withTime(Instant.ofEpochSecond(0L, System.currentTimeMillis()).toString())
                .withValue(1.1)
                .withAttributes(attributeMap)
                .withData("{\"message\":\"c3f847eb-333a-49c3-a4cd-54715ad1b58a\"}")
                .withEventMetadata(eventMetadata).build();
        event.setEventHandle(mock(EventHandle.class));
        return new Record<>(event);
    }

    @Test
    public void http_endpoint_test_with_single_record() throws NoSuchFieldException, IllegalAccessException {
        final PrometheusSinkService prometheusSinkService = createPrometheusSinkServiceUnderTest();
        final Collection<Record<Event>> records = setEventQueue(1);
        prometheusSinkService.output(records);
        assertDoesNotThrow(() -> { prometheusSinkService.output(records);});
    }

}
