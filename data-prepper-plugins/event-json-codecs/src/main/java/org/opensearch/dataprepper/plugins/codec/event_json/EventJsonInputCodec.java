/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.event_json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.Map;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@link InputCodec} which parses JSON Objects for arrays.
 */
@DataPrepperPlugin(name = "event_json", pluginType = InputCodec.class, pluginConfigurationType = EventJsonInputCodecConfig.class)
public class EventJsonInputCodec implements InputCodec {
    private static final Logger LOG = LoggerFactory.getLogger(JacksonEvent.class);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final JsonFactory jsonFactory = new JsonFactory();
    private final Boolean overrideTimeReceived;

    @DataPrepperPluginConstructor
    public EventJsonInputCodec(final EventJsonInputCodecConfig config) {
        this.overrideTimeReceived = config.getOverrideTimeReceived();
    }

    private boolean isCompatibleVersion(Map<String, Object> json) {
        final String versionStr = (String)json.get(EventJsonDefines.VERSION);
        final String[] version = versionStr.split("[.]");
        final DataPrepperVersion currentVersion = DataPrepperVersion.getCurrentVersion();

        final DataPrepperVersion definedVersion = DataPrepperVersion.parse(versionStr);
        if(definedVersion.getMajorVersion() != currentVersion.getMajorVersion()) {
          LOG.error("Version mismatch! Current version {} Received data version {}", currentVersion, versionStr);
            return false;
        }

        return true;
    }

    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final JsonParser jsonParser = jsonFactory.createParser(inputStream);

        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {
            if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);
                if (!isCompatibleVersion(innerJson)) {
                    return;
                }
                final List<Map<String, Object>> events = (List<Map<String, Object>>)innerJson.get(EventJsonDefines.EVENTS);
                for (Map<String, Object> eventMap: events) {
                    final Record<Event> record = createRecord(eventMap);
                    if (record != null) {
                        eventConsumer.accept(record);
                    }
                }
            }
        }
    }

    private Record<Event> createRecord(final Map<String, Object> innerJson) {
        Map<String, Object> metadata = (Map<String, Object>)innerJson.get(EventJsonDefines.METADATA);
        EventMetadata eventMetadata = objectMapper.convertValue(metadata, DefaultEventMetadata.class);
        Map<String, Object> data = (Map<String, Object>)innerJson.get(EventJsonDefines.DATA);
        if (data == null) {
            return null;
        }
        if (!overrideTimeReceived) {
            eventMetadata = new DefaultEventMetadata.Builder()
                .withEventType(EventType.LOG.toString())
                .withAttributes(eventMetadata.getAttributes())
                .withTimeReceived(Instant.now())
                .withTags(eventMetadata.getTags())
                .withExternalOriginationTime(eventMetadata.getExternalOriginationTime())
                .build();
        }
        final JacksonLog.Builder logBuilder = JacksonLog.builder()
            .withData(data)
            .withEventMetadata(eventMetadata)
            .getThis();
        final JacksonEvent event = (JacksonEvent)logBuilder.build();
        final Record<Event> record =  new Record<>(event);
        return record;
    }
}
