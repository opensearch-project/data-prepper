package org.opensearch.dataprepper.plugins.kafka.source.deserializer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import com.fasterxml.jackson.databind.JsonNode;

class KafkaSourceJsonDeserializerTest {

	@Mock
	KafkaSourceJsonDeserializer kafkaSourceJsonDeserializer;

	@BeforeEach
	void setUp() throws Exception {
		kafkaSourceJsonDeserializer = new KafkaSourceJsonDeserializer();
	}

	@Test
	void test_json_deserilaize_with_valid_data() throws Exception {
		byte[] msg = "{\"city\":\"chicago\",\"name\":\"jon doe\"}".getBytes();
		String topic = "my-topic";
		JsonNode node = kafkaSourceJsonDeserializer.deserialize(topic, msg);
		assertNotNull(node);
	}

	@Test
	void test_json_deserilaize_with_null_data() throws Exception {
		byte[] msg = null;
		String topic = "my-topic";
		JsonNode node = kafkaSourceJsonDeserializer.deserialize(topic, msg);
		assertNull(node);
	}

	@Test
	void test_json_deserilaize_with_exception() throws Exception {
		byte[] msg = "JSONDATA".getBytes();
		String topic = "my-topic";
		Assertions.assertThrows(Exception.class, () -> kafkaSourceJsonDeserializer.deserialize(topic, msg));
	}
}
