/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaConfigTest {

	@Mock
	SchemaConfig schemaConfig;

	@BeforeEach
	void setUp() throws IOException {
		schemaConfig = new SchemaConfig();
		//Added to load Yaml file - Start
		Yaml yaml = new Yaml();
		FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
		Object data = yaml.load(fileReader);
		ObjectMapper mapper = new ObjectMapper();
		if(data instanceof Map){
			Map<String, Object> propertyMap = (Map<String, Object>) data;
			Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
			Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
			Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
			mapper.registerModule(new JavaTimeModule());
			String json = mapper.writeValueAsString(kafkaConfigMap);
			Reader reader = new StringReader(json);
			KafkaSourceConfig kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
			schemaConfig = kafkaSourceConfig.getSchemaConfig();
		}
	}

	@Test
	void test_schemaConfig_not_null() {
		assertThat(schemaConfig, notNullValue());
		assertThat(schemaConfig.getVersion(), notNullValue());
		assertThat(schemaConfig.getRegistryURL(), notNullValue());
		assertThat(schemaConfig.getSchemaRegistryApiKey(), notNullValue());
		assertThat(schemaConfig.getSchemaRegistryApiSecret(), notNullValue());
	}

	@Test
	void test_schemaConfig_values() {
		assertEquals(1, schemaConfig.getVersion());
		assertEquals("http://localhost:8081/", schemaConfig.getRegistryURL());
	}

}
