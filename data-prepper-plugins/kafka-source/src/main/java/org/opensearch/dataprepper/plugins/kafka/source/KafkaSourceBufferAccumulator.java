/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.source.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * * A helper utility class which helps to write different formats of records
 * like json and string to the buffer.
 */
@SuppressWarnings("deprecation")
public class KafkaSourceBufferAccumulator<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceBufferAccumulator.class);
  private static final String MESSAGE_KEY = "message";
  private TopicConfig topicConfig;
  private PluginMetrics pluginMetrics;
  private final Counter kafkaConsumerWriteError;
  private static final String KAFKA_CONSUMER_BUFFER_WRITE_ERROR = "kafkaConsumerBufferWriteError";
  private final JsonFactory jsonFactory = new JsonFactory();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public KafkaSourceBufferAccumulator(TopicConfig topicConfigs, PluginMetrics pluginMetric) {
    this.topicConfig = topicConfigs;
    this.pluginMetrics = pluginMetric;
    this.kafkaConsumerWriteError = pluginMetrics.counter(KAFKA_CONSUMER_BUFFER_WRITE_ERROR);
  }

  public Record<Object> getEventRecord(final String line, TopicConfig topicConfig) {
    Map<String, Object> message = new HashMap<>();
    if (MessageFormat.getByMessageFormatByName(topicConfig.getSchemaConfig().getSchemaType())
            .equals(MessageFormat.PLAINTEXT)) {
      message.put(MESSAGE_KEY, line);
    } else if (MessageFormat.getByMessageFormatByName(topicConfig.getSchemaConfig().getSchemaType())
            .equals(MessageFormat.JSON)) {
      try {
        final JsonParser jsonParser = jsonFactory.createParser(line);
        message = objectMapper.readValue(jsonParser, Map.class);
      } catch (Exception e) {
        LOG.error("Unable to parse json data [{}], assuming plain text", line, e);
        message.put(MESSAGE_KEY, line);
      }

    }
    Event event = JacksonLog.builder().withData(message).build();
    return new Record<>(event);
  }

  public synchronized void writeAllRecordToBuffer(List<Record<Object>> kafkaRecords, final Buffer<Record<Object>> buffer, TopicConfig topicConfig) {
    try {
      buffer.writeAll(kafkaRecords,
              topicConfig.getConsumerGroupConfig().getBufferDefaultTimeout().toSecondsPart());
      LOG.info("Total number of records publish in buffer {} for Topic : {}", kafkaRecords.size(),topicConfig.getName());
    } catch (Exception e) {
      LOG.error("Error occurred while writing data to the buffer {}", e.getMessage());
      kafkaConsumerWriteError.increment();
    }
  }
}
