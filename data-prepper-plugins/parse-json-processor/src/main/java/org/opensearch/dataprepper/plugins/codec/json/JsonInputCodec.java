/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.codec.JsonDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An implementation of {@link InputCodec} which parses JSON Objects for arrays.
 */
@DataPrepperPlugin(name = "json", pluginType = InputCodec.class)
public class JsonInputCodec extends JsonDecoder implements InputCodec {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();

    @Override
    public void parse(
            final InputFile inputFile,
            final DecompressionEngine decompressionEngine,
            final Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputFile);
        Objects.requireNonNull(eventConsumer);

        parse(decompressionEngine.createInputStream(inputFile.newStream()), eventConsumer);
    }

}
