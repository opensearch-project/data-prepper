/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.sink.KafkaSinkConfig;
import org.springframework.test.util.ReflectionTestUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
public class SinkPropertyConfigurerTest {
    KafkaSinkConfig kafkaSinkConfig;

    MockedStatic<AuthenticationPropertyConfigurer> authenticationPropertyConfigurerMockedStatic;


    @BeforeEach
    public void setUp() throws IOException {

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

        authenticationPropertyConfigurerMockedStatic = mockStatic(AuthenticationPropertyConfigurer.class);
    }

    @AfterEach
    public void postProcess() {
        authenticationPropertyConfigurerMockedStatic.close();
    }

    @Test
    public void testGetProducerPropertiesForJson() {
        ReflectionTestUtils.setField(kafkaSinkConfig, "schemaConfig", null);
        Properties props = SinkPropertyConfigurer.getProducerProperties(kafkaSinkConfig);
        Assertions.assertEquals("30000", props.getProperty("session.timeout.ms"));
    }

    @Test
    public void testGetProducerPropertiesForPlainText() {
        ReflectionTestUtils.setField(kafkaSinkConfig, "serdeFormat", "plaintext");
        Assertions.assertThrows(RuntimeException.class, () -> SinkPropertyConfigurer.getProducerProperties(kafkaSinkConfig));

    }

    @Test
    public void testGetProducerPropertiesForAvro() {
        ReflectionTestUtils.setField(kafkaSinkConfig, "serdeFormat", "avro");
        SchemaConfig schemaConfig = kafkaSinkConfig.getSchemaConfig();
        ReflectionTestUtils.setField(kafkaSinkConfig, "schemaConfig", null);
        // Assertions.assertThrows(RuntimeException.class, () -> SinkPropertyConfigurer.getProducerProperties(kafkaSinkConfig));
        ReflectionTestUtils.setField(kafkaSinkConfig, "schemaConfig", schemaConfig);
        Assertions.assertEquals("30000", SinkPropertyConfigurer.getProducerProperties(kafkaSinkConfig).getProperty("session.timeout.ms"));

    }

    @Test
    public void testGetProducerPropertiesForNoSerde() {
        ReflectionTestUtils.setField(kafkaSinkConfig, "serdeFormat", null);
        Assertions.assertThrows(RuntimeException.class, () -> SinkPropertyConfigurer.getProducerProperties(kafkaSinkConfig));
    }

}
