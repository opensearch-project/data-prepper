/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A Data Prepper {@link InputCodec} which reads ND-JSON and other similar
 * formats which have JSON objects together.
 */
@DataPrepperPlugin(name = "ndjson", pluginType = InputCodec.class, pluginConfigurationType = NdjsonInputConfig.class)
public class NdjsonInputCodec implements InputCodec {
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {};
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final NdjsonInputConfig ndjsonInputConfig;
    private final EventFactory eventFactory;
    private final JsonFactory jsonFactory;

    @DataPrepperPluginConstructor
    public NdjsonInputCodec(final NdjsonInputConfig ndjsonInputConfig, final EventFactory eventFactory) {
        this.ndjsonInputConfig = ndjsonInputConfig;
        this.eventFactory = eventFactory;
        jsonFactory = new JsonFactory();
    }

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream, "Parameter inputStream must not be null.");
        Objects.requireNonNull(eventConsumer, "Parameter eventConsumer must not be null.");

        final JsonParser parser = jsonFactory.createParser(inputStream);

        final MappingIterator<Map<String, Object>> mapMappingIterator = objectMapper.readValues(parser, MAP_TYPE_REFERENCE);
        while (mapMappingIterator.hasNext()) {
            final Map<String, Object> json = mapMappingIterator.next();

            if(!ndjsonInputConfig.isIncludeEmptyObjects() && json.isEmpty())
                continue;

            final Record<Event> record = createRecord(json);
            eventConsumer.accept(record);
        }
    }

    private Record<Event> createRecord(final Map<String, Object> json) {
        final Log event = eventFactory.eventBuilder(LogEventBuilder.class)
                .withData(json)
                .build();

        return new Record<>(event);
    }
}
