package org.opensearch.dataprepper.plugins.kafka.configuration;

import static org.junit.Assert.assertEquals;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.yaml.snakeyaml.Yaml;

class SchemaConfigTest {

	@Mock
	SchemaConfig schemaConfig;

	private String keyDeserializer;

	private String valueDeserializer;
	private String recordType;
	private String schemaType;
	private String registryUrl;



	@BeforeEach
	void setUp() throws IOException {
		schemaConfig = new SchemaConfig();
		//Added to load Yaml file - Start
		Yaml yaml = new Yaml();
		FileReader reader = new FileReader(getClass().getClassLoader().getResource("sample-pipelines.yaml").getFile());
		Object data = yaml.load(reader);
		ObjectMapper mapper = new ObjectMapper();
		if(data instanceof Map){
			Map<Object, Object> propertyMap = (Map<Object, Object>) data;
			Map<Object, Object> nestedMap = new HashMap<>();
			iterateMap(propertyMap, nestedMap);
			List<TopicConfig> topicConfigList = (List<TopicConfig>) nestedMap.get("topics");
			mapper.registerModule(new JavaTimeModule());
			TopicsConfig topicsConfig = mapper.convertValue(topicConfigList.get(0), TopicsConfig.class);
			TopicConfig topicConfig = topicsConfig.getTopic();
			schemaConfig = topicConfig.getSchemaConfig();
			keyDeserializer = schemaConfig.getKeyDeserializer();
			valueDeserializer = schemaConfig.getValueDeserializer();
			recordType = schemaConfig.getRecordType();
			schemaType = schemaConfig.getSchemaType();
			registryUrl = schemaConfig.getRegistryURL();
		}
	}

	private static Map<Object, Object> iterateMap(Map<Object, Object> map, Map<Object, Object> nestedMap) {
		for (Map.Entry<Object, Object> entry : map.entrySet()) {
			String key = (String) entry.getKey();
			Object value = entry.getValue();
			if (value instanceof Map) {
				iterateMap((Map<Object, Object>) value, nestedMap); // call recursively for nested maps
			} else {
				nestedMap.put(key, value);
			}
		}
		return nestedMap;
	}

	@Test
	void testConfigValues() {
		assertEquals(schemaConfig.getKeyDeserializer(), keyDeserializer);
		assertEquals(schemaConfig.getValueDeserializer(), valueDeserializer);
		assertEquals(schemaConfig.getRecordType(), recordType);
		assertEquals(schemaConfig.getSchemaType(), schemaType);
		assertEquals(schemaConfig.getRegistryURL(), registryUrl);

	}

}
