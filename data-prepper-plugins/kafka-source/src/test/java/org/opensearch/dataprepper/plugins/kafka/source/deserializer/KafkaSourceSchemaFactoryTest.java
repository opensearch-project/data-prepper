package org.opensearch.dataprepper.plugins.kafka.source.deserializer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.plugins.kafka.source.util.MessageFormat;

class KafkaSourceSchemaFactoryTest {

	@Mock
	KafkaSourceSchemaFactory kafkaSourceSchemaFactory;

	@BeforeEach
	void setUp() throws Exception {
		kafkaSourceSchemaFactory = new KafkaSourceSchemaFactory();
	}

	@SuppressWarnings("static-access")
	@Test
	void test_schema_factory_with_json_type() throws Exception {
		MessageFormat format = MessageFormat.JSON;
		assertNotNull(kafkaSourceSchemaFactory.getSchemaType(format));
	}

	@SuppressWarnings("static-access")
	@Test
	void test_schema_factory_with_plaintext_type() throws Exception {
		MessageFormat format = MessageFormat.PLAINTEXT;
		assertNotNull(kafkaSourceSchemaFactory.getSchemaType(format));
	}

	@SuppressWarnings("static-access")
	@Test
	void test_schema_factory_with_invalid_type() throws Exception {
		MessageFormat format = null;
		Assertions.assertThrows(Exception.class, () -> kafkaSourceSchemaFactory.getSchemaType(format));
	}

}
