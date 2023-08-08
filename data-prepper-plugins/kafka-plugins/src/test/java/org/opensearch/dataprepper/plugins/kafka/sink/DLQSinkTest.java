/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.sink;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DLQSinkTest {

    @Mock
    private PluginFactory pluginFactory;


    private KafkaSinkConfig kafkaSinkConfig;

    @Mock
    private DlqProvider dlqProvider;

    @Mock
    private DlqWriter dlqWriter;


    private PluginSetting pluginSetting;

    private DLQSink dlqSink;

    @BeforeEach
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(pluginFactory.loadPlugin(any(Class.class), any(PluginSetting.class))).thenReturn(dlqProvider);
        when(dlqProvider.getDlqWriter(anyString())).thenReturn(Optional.of(dlqWriter));
        pluginSetting = new PluginSetting("kafka-sink", new HashMap<>());


        Yaml yaml = new Yaml();
        FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines-sink.yaml").getFile());
        Object data = yaml.load(fileReader);
        if (data instanceof Map) {
            Map<String, Object> propertyMap = (Map<String, Object>) data;
            Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
            Map<String, Object> sinkeMap = (Map<String, Object>) logPipelineMap.get("sink");
            Map<String, Object> kafkaConfigMap = (Map<String, Object>) sinkeMap.get("kafka");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String json = mapper.writeValueAsString(kafkaConfigMap);
            Reader reader = new StringReader(json);
            kafkaSinkConfig = mapper.readValue(reader, KafkaSinkConfig.class);


        }
        dlqSink = new DLQSink(pluginFactory, kafkaSinkConfig, pluginSetting);
    }

    @Test
    public void testPerform() throws IOException {
        Object failedData = new Object();
        ReflectionTestUtils.setField(pluginSetting, "pipelineName", "test");
        doNothing().when(dlqWriter).write(anyList(), anyString(), anyString());
        dlqSink.perform(failedData, mock(Exception.class));
        verify(dlqWriter).write(anyList(), anyString(), anyString());
    }


}
