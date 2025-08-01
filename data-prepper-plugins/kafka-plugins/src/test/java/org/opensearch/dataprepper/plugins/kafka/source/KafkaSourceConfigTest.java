/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.IsolationLevel;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class KafkaSourceConfigTest {

	@Mock
    KafkaSourceConfig kafkaSourceConfig;

	List<String> bootstrapServers;

	@BeforeEach
	void setUp() throws IOException {
		//Added to load Yaml file - Start
		Yaml yaml = new Yaml();
		FileReader fileReader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
		Object data = yaml.load(fileReader);
		if(data instanceof Map){
			Map<String, Object> propertyMap = (Map<String, Object>) data;
			Map<String, Object> logPipelineMap = (Map<String, Object>) propertyMap.get("log-pipeline");
			Map<String, Object> sourceMap = (Map<String, Object>) logPipelineMap.get("source");
			Map<String, Object> kafkaConfigMap = (Map<String, Object>) sourceMap.get("kafka");
			ObjectMapper mapper = new ObjectMapper();
			mapper.registerModule(new JavaTimeModule());
			String json = mapper.writeValueAsString(kafkaConfigMap);
			Reader reader = new StringReader(json);
			kafkaSourceConfig = mapper.readValue(reader, KafkaSourceConfig.class);
		}
	}

	@Test
	void test_kafka_config_not_null() {
		assertThat(kafkaSourceConfig, notNullValue());
	}

	@Test
	void test_bootStrapServers_not_null(){
		assertThat(kafkaSourceConfig.getBootstrapServers(), notNullValue());
		String bootstrapServers = kafkaSourceConfig.getBootstrapServers().get(0);
		assertTrue(bootstrapServers.contains("127.0.0.1:9093"));
	}

	@Test
	void test_topics_not_null(){
		assertEquals(false, kafkaSourceConfig.getAcknowledgementsEnabled());
		assertThat(kafkaSourceConfig.getTopics(), notNullValue());
	}

	@Test
	void test_setters() throws NoSuchFieldException, IllegalAccessException {
		kafkaSourceConfig = new KafkaSourceConfig();
        EncryptionConfig encryptionConfig = kafkaSourceConfig.getEncryptionConfig();
		kafkaSourceConfig.setBootStrapServers(new ArrayList<>(Arrays.asList("127.0.0.1:9092")));
		SourceTopicConfig topicConfig = mock(SourceTopicConfig.class);
		kafkaSourceConfig.setTopics(Collections.singletonList(topicConfig));

		assertEquals(Collections.singletonList("127.0.0.1:9092"), kafkaSourceConfig.getBootstrapServers());
		assertEquals(Collections.singletonList(topicConfig), kafkaSourceConfig.getTopics());
        setField(KafkaSourceConfig.class, kafkaSourceConfig, "acknowledgementsEnabled", true);
		assertEquals(true, kafkaSourceConfig.getAcknowledgementsEnabled());
		Assertions.assertEquals(EncryptionType.SSL, kafkaSourceConfig.getEncryptionConfig().getType());
        setField(EncryptionConfig.class, encryptionConfig, "type", EncryptionType.NONE);
		assertEquals(EncryptionType.NONE, encryptionConfig.getType());
        setField(EncryptionConfig.class, encryptionConfig, "type", EncryptionType.SSL);
		assertEquals(EncryptionType.SSL, encryptionConfig.getType());
	}

	@Test
	void test_isolation_level_deserialization_from_yaml() {
		assertThat(kafkaSourceConfig.getTopics(), notNullValue());
		SourceTopicConfig topic = (SourceTopicConfig) kafkaSourceConfig.getTopics().get(0);
		assertEquals(IsolationLevel.READ_COMMITTED, topic.getIsolationLevel());
	}
}
