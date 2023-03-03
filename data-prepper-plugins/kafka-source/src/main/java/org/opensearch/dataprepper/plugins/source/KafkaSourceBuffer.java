/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaSourceBuffer {

	private static final Logger logger = LoggerFactory.getLogger(KafkaSourceBuffer.class);
	private static final String MESSAGE_KEY = "message";
	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static final TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() {
	};
	private final KafkaSourceConfig sourceConfig;

	public KafkaSourceBuffer(KafkaSourceConfig sourceConfig) {
		this.sourceConfig = sourceConfig;
	}

	@SuppressWarnings({ "deprecation" })
	private Record<Object> getEventRecord(final String line) {// TODO this param should be a generic type
		Map<String, Object> structuredLine = new HashMap<>();

		switch (MessageFormat.getByName(sourceConfig.getSchemaType())) {
		case JSON:
			structuredLine = parseJson(line);
			break;
		case STRING:
			structuredLine.put(MESSAGE_KEY, line);
			break;
		default:
			break;
		}

		return new Record<>(
				JacksonEvent.builder().withEventType(sourceConfig.getRecordType()).withData(structuredLine).build());
	}

	private Map<String, Object> parseJson(final String jsonString) {
		try {
			return objectMapper.readValue(jsonString, typeReference);
		} catch (JsonProcessingException e) {
			logger.error("Unable to parse json data [{}], assuming plain text", jsonString, e);
			final Map<String, Object> plainMap = new HashMap<>();
			plainMap.put(MESSAGE_KEY, jsonString);
			return plainMap;
		}
	}

	@SuppressWarnings("deprecation")
	public void writeEventOrStringToBuffer(String line, final Buffer<Record<Object>> buffer)
			throws TimeoutException, IllegalArgumentException {

		buffer.write(getEventRecord(line), sourceConfig.getBufferDefaultTimeout().toSecondsPart());

	}
}
